package main

/*
* To run,
  
  > go run crawler.go -basedir=<directory to save files> -maxdepth <Maximum depth> http://<url> <throttle_delay> 
  
Example :

  > go run crawler.go -basedir="google" -maxdepth 4 http://google.com 5s  
  
To check database : 

  > sqlite3 google/sqlite.db "select * from url_index;"

  
  This program creates an sqlite3 database named "sqlite.db" in base directory
  which contains a table named "url_index" with columns as follows
  -----------------------------------------------
  | url  | visited (date and time) | path (md5) |
  -----------------------------------------------

  * "visited" and "path" are null for unvisited urls

  * Urls are converted to MD5 hash for unique filenames


*/

import (
  
  "crypto/tls" 
  "crypto/md5"
  "encoding/hex" 
  "flag"
  "fmt"
  "net/http"
  "net/url"
  "os"
  "io/ioutil"
  "strings"
  "time"
  "code.google.com/p/go-sqlite/go1/sqlite3"
)


                                     
var visited = make(map[string]bool)  
var sleeptime time.Duration
var DB_CONNECTION *sqlite3.Conn
var DB_TABLE_NAME = "url_index"
var DB_FILE_NAME = "sqlite.db"
var BASE_DIR string
var MAX_DEPTH int

func main() {

  // get max_depth from command line flag --maxdepth
  MAX_DEPTH_ptr := flag.Int("maxdepth", 3, "Maximum depth to be parsed")
  BASE_DIR_ptr := flag.String("basedir", "tmp", "Base directory to store pages")

  flag.Parse()

  BASE_DIR = *BASE_DIR_ptr
  MAX_DEPTH = *MAX_DEPTH_ptr

  //Parse command line flags
  args := flag.Args()
  fmt.Println(args)
  if len(args) < 1 {
    fmt.Println("Please specify start page")
    os.Exit(1)
  }

  
  fmt.Println("Base: " + BASE_DIR)
  fmt.Println("Making dir " + BASE_DIR)
  // create base directory
  makeDirectory(BASE_DIR)
  DB_FILE_NAME =  BASE_DIR + "/"+ DB_FILE_NAME

  // get throttle delay from second command line argument
  if len(args) < 2 {
    // if argument not supplied, fallback to default value (1s)
    sleeptime = 1  
  
  } else {
    sleeptime, _ = time.ParseDuration(args[1])
  }

  // Create database connection
  c, error := sqlite3.Open(DB_FILE_NAME)
  if error != nil {
    fmt.Println("Error opening database sqlite.db")
    os.Exit(1)
  }
  DB_CONNECTION = c


  err := DB_CONNECTION.Exec("select * from " + DB_TABLE_NAME)
  if err != nil {
    
    if strings.Contains(err.Error(), "no such table"){
      // table doesn't exist
      fmt.Println("Table "+ DB_TABLE_NAME +" doesn't exist, creating")
      DB_CONNECTION.Exec("create table "+ DB_TABLE_NAME + " ( url text unique not null, visited datetime default current_timestamp, path default null);")
      fmt.Println("Table created")
      } else {
        fmt.Println("Unknown error occured:\n\t" + err.Error())
      }
  }



   
  queue := make(chan string)
  go func() { queue <- args[0] }()                //insert base url into queue
  getUnvisitedUrlsAndEnqueue(queue, MAX_DEPTH)    // get unvisited urls from Database and enque them
  insertUrl(args[0])
  for uri := range queue { 
    enqueue(uri, queue, 0, MAX_DEPTH)
     if len(queue) == 0 {
      os.Exit(0)
    }
  }
}

func enqueue(uri string, queue chan string, depth int, MAX_DEPTH int) {
  
  if checkUrl(uri){
    // if url already visited
    if checkVisited(uri){
      return
    }
  }

  if depth >= MAX_DEPTH { return }
  
  if !visited[uri] {
    fmt.Println("++++++++++++++++++++++++++++++++++")
    fmt.Println("fetching", uri)

    
    insertUrl(uri)    // create row for url
    defer insertUrl(uri)  // update timestamp for url

    transport := &http.Transport{
      TLSClientConfig: &tls.Config{
        InsecureSkipVerify: true,
      },
    }

    time.Sleep(sleeptime)         //Specify delay duration
    
    client := http.Client{Transport: transport} // create HTTP client object
    resp, err := client.Get(uri)  // send GET request to URI
    if err != nil { // Error occured
      //fmt.Println(err)
      return
    }

    defer resp.Body.Close()


     
    if resp.StatusCode == 200{  // Successful GET request
      body, _ := ioutil.ReadAll(resp.Body)
      file_name := GetMD5Hash(uri)
      updateFilename(uri, BASE_DIR + "/" + file_name)
      
      fmt.Println("Saving file: " + file_name)
      save_file(body, BASE_DIR + "/" + file_name)
      
      body1 :=string(body)

      links := get_attr(body1, "href")
      images := get_attr(body1, "src")
      for i := range(images){
        links = append(links, images[i])
      }
      
      for i:= 0; i < len(links); i++ {
        link := fixUrl(links[i], uri)
        
        if link != uri{
          visited[link] = false
        }

        // if link not in database
        //    enqueue
        //    add link to db
        if !checkUrl(link){
          insertUrl(link)
          enqueue(link, queue, depth + 1, MAX_DEPTH)
        }
      }
    }
  }
}

/*
 *  Get all attribute values from given html string
 */
func get_attr(body string, attr string) ([]string){
  attrs := strings.Split(body, attr)[1:]
  var ret []string
  for i := 0; i<len(attrs); i++ {
    attr := strings.Trim(attrs[i], " =")
    if len(attr) < 2 { break }
    quote := attr[0:1]
    if quote == "\"" || quote == "'"{
      attr = attr[1:]
      last :=  strings.Index(attr, quote)
      if last <= 0 {
        continue
      }
      attr = attr[:last]
      if len(attr)>1{
        ret = append(ret, attr)
      }
    }
  }
  return ret
}

// Saves byte array to file 
func save_file(body []byte, file_path string) {
  fmt.Println("Saving file: " + file_path)
  f1, err := os.Create(file_path)
  if err != nil {
    
    fmt.Println("Error while creating file " + file_path + ": " + err.Error())
  }
  fmt.Println("==================\nWriting file: " + file_path + "\n======================")
  _, err = f1.Write(body)
  if err != nil {
    fmt.Println("Error while writing file "+ file_path + ": " + err.Error())
  }
  defer f1.Close()
}



/*
 *  Retreive list of unvisited urls from database and enque them for downloading
 */
func getUnvisitedUrlsAndEnqueue(queue chan string, MAX_DEPTH int) {
  rows, err := DB_CONNECTION.Query("select url from " + DB_TABLE_NAME + " where visited is null")
  // defer rows.Close()
  if err != nil {
    fmt.Println("Error: " + err.Error())
    if err.Error() == "EOF" {
      return
    }
  }
  var url string
  error := rows.Scan(&url)
  for  error == nil{
    fmt.Println("Inserting into queue: " + url)
    enqueue(url, queue, 0, MAX_DEPTH)
    error = rows.Scan(&url)
    rows.Next() 
  }
  rows.Close()
}



/*
 * Inserts a new URL into database
 * if URL already exists in db, update the visited timestamp
*/
func insertUrl(url string){
  error := DB_CONNECTION.Exec("insert into " + DB_TABLE_NAME + " values (\"" + url + "\", NULL, NULL);")
  if error != nil {
    if strings.Contains(error.Error(), "column url is not unique" ){
      // url already exists in DB
      DB_CONNECTION.Exec("update " + DB_TABLE_NAME + " set visited=current_timestamp where url=\"" + url + "\";")
    } else {
      fmt.Println("Error in function insertUrl(): " + error.Error() )
      return
    }
  }
}
/*
 * Updates filename column of url in database
 */
func updateFilename(url string, filename string) {
  err := DB_CONNECTION.Exec("update " + DB_TABLE_NAME + " set path=\"" + filename + "\" where url=\"" + url + "\";")
  if err != nil {
    fmt.Println("Error while updating filepath: " + err.Error())
  }
}



/*
 *  Returns true if url exists in database
 *  returns false otherwise
 */
func checkUrl(url string) (bool) {
  // fmt.Println("Start check url: " + url)
  rows, error := DB_CONNECTION.Query("select * from " + DB_TABLE_NAME + " where url=\"" + url + "\";")

  if error != nil {
    return false  
  }
  
  if rows == nil {
    return false
  } else {
    return true
  }
}


/*
 *  Returns true if given url has non null visited timestamp
 *  returns false otherwise
 */
func checkVisited(url string) (bool) {
  rows, error := DB_CONNECTION.Query("select * from " + DB_TABLE_NAME + " where url=\"" + url + "\";")
  
  if error != nil {
    return false  
  }
  
  if rows == nil { // no rows returned
    return false
  } else {
    var u string
    var timestamp time.Time
    rows.Scan(&u, &timestamp)
    
    if timestamp.IsZero() { // if timestamp column is null, url is unvisited
      return false
    } else {
      return true
    }
  }
}  

/*
 * Make directory if it doesn't already exist
 */
func makeDirectory(name string) {
  e := os.MkdirAll(name, (1<<31)|0x1ff)
  if e != nil {
    fmt.Println("Error while creating directory " + name + ": " + e.Error())
    
  }
}

/*
 *  Converts a url to a unique filename
 */
func GetMD5Hash(text string) string {
  hasher := md5.New()
  hasher.Write([]byte(text))
  return hex.EncodeToString(hasher.Sum(nil))
}
/*
 * Change relative links to absolute 
 */
func fixUrl(href, base string) (string) {
  uri, err := url.Parse(href)
  if err != nil {
    return ""
  }
  baseUrl, err := url.Parse(base)
  if err != nil {
    return ""
  }
  uri = baseUrl.ResolveReference(uri)
  return uri.String()
}
