package main

import (
	"fmt"
	"os"
	"log"
	"bytes"
	// "reflect"
	// "strings"
	"net/url"
    "net/http"
	"hash/fnv"
    "io/ioutil"
	"encoding/json"
	"encoding/base64"
)

type errorResp struct {
	Code int
	Message string
}

type serverKeyValuePair struct {
	Key string
	Value string
}

type keyValuePair struct {
	Key string
	Value string
}

type clientReq struct {
	Encoding string
	Data string
}

type setReqStruct struct {
	Key clientReq
	Value clientReq
}

type fetchReqStruct struct {
	Key clientReq
}

type queryReqStruct struct {
	Key clientReq
}

type server struct {
	Ip string
    Port  int
}

var url1 string
var servers []server
var setReqs []setReqStruct

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
	return
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
	myUrl := fmt.Sprintf("http://%s:%d/fetch?key=%s", myServer.Ip, myServer.Port, key)
	fmt.Println(myUrl)
	resp, err := http.Get(myUrl)
	
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
	body, readErr := ioutil.ReadAll(r.Body)
    if readErr != nil {
        log.Fatal(readErr)
	}
	var setReqs []setReqStruct
	json.Unmarshal(body, &setReqs)

	// massage the body
	isValid := true
	serverReqMap := make(map[int][]serverKeyValuePair)
	for i := 0; i < len(setReqs); i++ {
		// get server index & validate the encoding
		keyEncoding := setReqs[i].Key.Encoding
		keyVal := setReqs[i].Key.Data
		keyValStr := keyVal
		if keyEncoding == "binary" {
			keyValStr, isValid = binToStr(keyValStr);
		}
		if !isValid {
			break;
		}
		serverIdx := int(hash(keyValStr)) % len(servers)

		// create and append server request
		tmp := serverKeyValuePair{Key: keyValStr, Value: setReqs[i].Value.Data}
		serverReqMap[serverIdx] = append(serverReqMap[serverIdx], tmp)
	}

	// handle exception
	if !isValid {
		// TODO: return 4XX code here
		handleError(w, r, &errorResp{Code: 405, Message: "Bad key encoding."})
		return
	}

	// call the server(s) correspondingly
	for j := 0; j < len(servers); j++ {
		serverReqs := serverReqMap[j]
		// prepare the request
		serverEndpoint := fmt.Sprintf("http://%s:%d/set", servers[j].Ip, servers[j].Port)
		httpReq := compositeServerReq(serverEndpoint, serverReqs)
		go makeServerReq(httpReq)
		// TODO: handle response merging
	}

	// TODO: return status
}

/*
* Utility functions
*/

func handleError(w http.ResponseWriter, r *http.Request, errsp *errorResp) {
	js, err := json.Marshal(errsp)
	if err != nil {
	http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(errsp.Code)
	w.Write(js)
	return
}

func makeServerReq(httpReq *http.Request) {
	// TODO: pass to response handler
	if httpReq != nil {
		httpReq.Header.Set("Content-Type", "application/json")
		client := &http.Client{}
		resp, err := client.Do(httpReq)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
	}
}

func compositeServerReq(endpoint string, kvPairs []serverKeyValuePair) (*http.Request) {
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
	// TODO: add validation logic | is it really needed?
	realStr := base64.StdEncoding.EncodeToString([]byte(s))
	return realStr, false
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
	fmt.Printf("%+v\n", servers);
	fmt.Printf("hi %d\n", servers[0].Port)
}

func loadSetRequest(jsonBytes []byte) []setReqStruct {
	var setReqs []setReqStruct
	json.Unmarshal(jsonBytes, &setReqs)
	return setReqs;
}

func loadFetchRequest(jsonBytes []byte) []fetchReqStruct {
	var fetchReqs []fetchReqStruct
	json.Unmarshal(jsonBytes, &fetchReqs)
	return fetchReqs;
}

func loadQueryRequest(jsonBytes []byte) []queryReqStruct {
	var queryReqs []queryReqStruct
	json.Unmarshal(jsonBytes, &queryReqs)
	return queryReqs;
}

func main() {
	loadServers();
	http.HandleFunc("/", handler)
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}