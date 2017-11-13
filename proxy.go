package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"sync"
	// "reflect"
	// "strings"
	"encoding/base64"
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"net/http"
	"net/url"
)

type server struct {
	IP   string
	Port int
}

type errorResp struct {
	Code    int
	Message string
}

type serverResp struct {
	Key    string
	Status bool
}

type keyValuePair struct {
	Key   string
	Value string
}

type clientReq struct {
	Encoding string
	Data     string
}

type clientSetReq struct {
	Key   clientReq
	Value clientReq
}

type clientFetchReq struct {
	Key clientReq
}

type clientQueryReq struct {
	Key clientReq
}

var servers []server

func handler(w http.ResponseWriter, r *http.Request) {
	endpoint := r.URL.Path
	switch endpoint {
	case "/fetch":
		handleFetch(w, r)
	case "/query":
		handleQuery(w, r)
	case "/set":
		handleSet(w, r)
	}
}

func handleFetch(w http.ResponseWriter, r *http.Request) {
	// single fetch logic
	m, _ := url.ParseQuery(r.URL.RawQuery)
	key := m["key"][0]

	// dummy hash logic
	size := len(servers)
	hash := int(hash(key))
	serverIdx := hash % size
	myServer := servers[serverIdx]

	// call the server and extract result
	myURL := fmt.Sprintf("http://%s:%d/fetch?key=%s", myServer.IP, myServer.Port, key)
	resp, err := http.Get(myURL)

	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(1)
	}

	body, readErr := ioutil.ReadAll(resp.Body)
	if readErr != nil {
		log.Fatal(readErr)
	}

	var result []keyValuePair
	json.Unmarshal(body, &result)
	fmt.Printf("%+v\n", result)

	// TODO: return the server response to the client

}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	// TODO: do something
}

func handleSet(w http.ResponseWriter, r *http.Request) {
	// extract the key value pairs
	body := loadReqBody(r)
	setReqs := loadSetRequest(body)

	// massage the body
	isValid := true
	serverReqMap := make(map[int][]keyValuePair)
	for i := 0; i < len(setReqs); i++ {
		// get server index & validate the encoding
		keyEncoding := setReqs[i].Key.Encoding
		keyVal := setReqs[i].Key.Data
		keyValStr := keyVal
		if keyEncoding == "binary" {
			keyValStr, isValid = binToStr(keyValStr)
		}
		if !isValid {
			break
		}
		serverIdx := int(hash(keyValStr)) % len(servers)

		// create and append server request
		tmp := keyValuePair{Key: keyValStr, Value: setReqs[i].Value.Data}
		serverReqMap[serverIdx] = append(serverReqMap[serverIdx], tmp)
	}

	// handle exception
	if !isValid {
		handleError(w, r, &errorResp{Code: 405, Message: "Bad key encoding."})
		return
	}

	// call the server(s) correspondingly
	serverRespMap := make(map[int][]serverResp)
	var wg sync.WaitGroup
	for j := 0; j < len(servers); j++ {
		// if no request go to this server, skip
		serverReqs := serverReqMap[j]
		if len(serverReqs) == 0 {
			continue
		}
		// prepare the request
		serverEndpoint := fmt.Sprintf("http://%s:%d/set", servers[j].IP, servers[j].Port)
		httpReq := compositeServerReq(serverEndpoint, serverReqs)
		serverRespMap[j] = make([]serverResp, 0)
		// wait and request
		wg.Add(1)
		go makeServerReq(httpReq, &serverRespMap, j)
	}
	wg.Wait()

	// TODO: massage serverRespMap, stringify it and respond

	handleSuccess(w, r, []byte("haha"), 200) // TODO: handle partial content
}

/*
* Utility functions
 */

func handleSuccess(w http.ResponseWriter, r *http.Request, reply []byte, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(reply)
}

func handleError(w http.ResponseWriter, r *http.Request, errsp *errorResp) {
	js, err := json.Marshal(errsp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(errsp.Code)
	w.Write(js)
}

func makeServerReq(httpReq *http.Request, sRespMap *map[int][]serverResp, j int) {
	if httpReq != nil {
		httpReq.Header.Set("Content-Type", "application/json")
		client := &http.Client{}
		resp, err := client.Do(httpReq)
		if err != nil {
			panic(err)
		}
		// load the response and insert to sRespMap
		body := loadRespBody(resp)
		sresp := loadServerResp(body)
		(*sRespMap)[j] = sresp
		defer resp.Body.Close()
	}
}

func compositeServerReq(endpoint string, kvPairs []keyValuePair) *http.Request {
	jsonStr, err := json.Marshal(&kvPairs)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	req, httpErr := http.NewRequest("POST", endpoint, bytes.NewBuffer(jsonStr))
	if httpErr != nil {
		fmt.Println(httpErr)
		return nil
	}
	return req
}

func binToStr(s string) (string, bool) {
	// TODO: add validation for malformed binary | is it really needed?
	realStr := base64.StdEncoding.EncodeToString([]byte(s))
	return realStr, true
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func loadServers() {
	file, e := ioutil.ReadFile("servers.json")
	if e != nil {
		fmt.Printf("File error: %v\n", e)
		os.Exit(1)
	}
	json.Unmarshal(file, &servers)
}

func loadReqBody(r *http.Request) []byte {
	body, readErr := ioutil.ReadAll(r.Body)
	if readErr != nil {
		log.Fatal(readErr)
		return nil
	}
	return body
}

func loadRespBody(resp *http.Response) []byte {
	body, readErr := ioutil.ReadAll(resp.Body)
	if readErr != nil {
		log.Fatal(readErr)
	}
	return body
}

func loadServerResp(jsonBytes []byte) []serverResp {
	var serverResps []serverResp
	json.Unmarshal(jsonBytes, &serverResps)
	return serverResps
}

func loadSetRequest(jsonBytes []byte) []clientSetReq {
	var setReqs []clientSetReq
	json.Unmarshal(jsonBytes, &setReqs)
	return setReqs
}

func loadFetchRequest(jsonBytes []byte) []clientFetchReq {
	var fetchReqs []clientFetchReq
	json.Unmarshal(jsonBytes, &fetchReqs)
	return fetchReqs
}

func loadQueryRequest(jsonBytes []byte) []clientQueryReq {
	var queryReqs []clientQueryReq
	json.Unmarshal(jsonBytes, &queryReqs)
	return queryReqs
}

func main() {
	loadServers()
	http.HandleFunc("/", handler)
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
