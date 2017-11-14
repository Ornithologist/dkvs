package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
)

const success int = 200
const partialSuccess int = 206
const clientError int = 405
const serverError int = 500
const serverConfig string = "servers.json"

type massage func([]*http.Response) ([]byte, int)

type server struct {
	IP   string
	Port int
}

type errorResp struct {
	Code    int
	Message string
}

type serverSetResp struct {
	KeysAdded  int      `json:"keys_added"`
	KeysFailed []string `json:"keys_failed"`
}

type serverFetchResp struct {
	Key   string `json:"key"`
	Value bool   `json:"value"`
}

type serverQueryResp struct {
	Key   string `json:"key"`
	Value string `json:"value"`
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
	// extract the key value pairs
	body := loadReqBody(r)
	queryReqs := loadFetchRequest(body)

	// massage the body
	isValid := true
	serverReqMap := make(map[int][]string)
	for i := 0; i < len(queryReqs); i++ {
		// get server index & validate the encoding
		keyEncoding := queryReqs[i].Key.Encoding
		keyVal := queryReqs[i].Key.Data
		keyValStr := keyVal
		if keyEncoding == "binary" {
			keyValStr, isValid = binToStr(keyValStr)
		}
		if !isValid {
			break
		}
		serverIdx := int(hash(keyValStr)) % len(servers)
		// create and append server request
		serverReqMap[serverIdx] = append(serverReqMap[serverIdx], keyValStr)
	}

	// handle exception
	if !isValid {
		handleError(w, r, &errorResp{Code: clientError, Message: "Bad key encoding."})
		return
	}

	// assemble the requests (one per server)
	reqs := make([]*http.Request, 0)
	for j := 0; j < len(servers); j++ {
		// if no request goes to this server, skip
		serverReqs := serverReqMap[j]
		if len(serverReqs) == 0 {
			continue
		}
		// prepare the request
		serverEndpoint := fmt.Sprintf("http://%s:%d/fetch", servers[j].IP, servers[j].Port)
		httpReq := compositeServerReq(serverEndpoint, serverReqs)
		reqs = append(reqs, httpReq)
	}

	// send request, wait, massage, and sent response to client
	output, code := sendRequestsAndMassage(reqs, massageFetch)
	handleSuccess(w, r, output, code)
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	// extract the key value pairs
	body := loadReqBody(r)
	queryReqs := loadQueryRequest(body)

	// massage the body
	isValid := true
	serverReqMap := make(map[int][]string)
	for i := 0; i < len(queryReqs); i++ {
		// get server index & validate the encoding
		keyEncoding := queryReqs[i].Key.Encoding
		keyVal := queryReqs[i].Key.Data
		keyValStr := keyVal
		if keyEncoding == "binary" {
			keyValStr, isValid = binToStr(keyValStr)
		}
		if !isValid {
			break
		}
		serverIdx := int(hash(keyValStr)) % len(servers)

		// create and append server request
		serverReqMap[serverIdx] = append(serverReqMap[serverIdx], keyValStr)
	}

	// handle exception
	if !isValid {
		handleError(w, r, &errorResp{Code: clientError, Message: "Bad key encoding."})
		return
	}

	// assemble the requests (one per server)
	reqs := make([]*http.Request, 0)
	for j := 0; j < len(servers); j++ {
		// if no request goes to this server, skip
		serverReqs := serverReqMap[j]
		if len(serverReqs) == 0 {
			continue
		}
		// prepare the request
		serverEndpoint := fmt.Sprintf("http://%s:%d/query", servers[j].IP, servers[j].Port)
		httpReq := compositeServerReq(serverEndpoint, serverReqs)
		reqs = append(reqs, httpReq)
	}

	// send request, wait, massage, and sent response to client
	output, code := sendRequestsAndMassage(reqs, massageQuery)
	handleSuccess(w, r, output, code)
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
		handleError(w, r, &errorResp{Code: clientError, Message: "Bad key encoding."})
		return
	}

	// assemble the requests (one per server)
	reqs := make([]*http.Request, 0)
	for j := 0; j < len(servers); j++ {
		// if no request goes to this server, skip
		serverReqs := serverReqMap[j]
		if len(serverReqs) == 0 {
			continue
		}
		// prepare the request
		serverEndpoint := fmt.Sprintf("http://%s:%d/set", servers[j].IP, servers[j].Port)
		httpReq := compositeServerReq(serverEndpoint, serverReqs)
		reqs = append(reqs, httpReq)
	}

	// send request, wait, massage, and sent response to client
	output, code := sendRequestsAndMassage(reqs, massageSet)
	handleSuccess(w, r, output, code)
}

/*
 * Utility functions
 */
func massageFetch(resps []*http.Response) ([]byte, int) {
	final := make([]serverFetchResp, 0)
	code := success
	// aggregate
	for _, response := range resps {
		if response.StatusCode >= success {
			body := loadRespBody(response)
			sresp := loadServerFetchResp(body)
			// spread operator indicates list of arguments
			final = append(final, sresp...)
			// one server partial => all partial
			if response.StatusCode == partialSuccess {
				code = partialSuccess
			}
		} else {
			code = partialSuccess // TODO: wrong(!) shouldn't it return failure ?
		}
		response.Body.Close()
	}
	body, err := json.Marshal(final)
	if err != nil {
		return nil, serverError
	}
	return body, code
}

func massageQuery(resps []*http.Response) ([]byte, int) {
	final := make([]serverQueryResp, 0)
	code := success
	// aggregate
	for _, response := range resps {
		if response.StatusCode >= success {
			body := loadRespBody(response)
			sresp := loadServerQueryResp(body)
			// spread operator indicates list of arguments
			final = append(final, sresp...)
			// one server partial => all partial
			if response.StatusCode == partialSuccess {
				code = partialSuccess
			}
		} else {
			code = partialSuccess // TODO: wrong(!) shouldn't it return failure ?
		}
		response.Body.Close()
	}
	body, err := json.Marshal(final)
	if err != nil {
		return nil, serverError
	}
	return body, code
}

func massageSet(resps []*http.Response) ([]byte, int) {
	keysFailed := make([]string, 0)
	keysAdded := 0
	code := success
	// aggregate
	for _, response := range resps {
		if response.StatusCode >= success {
			body := loadRespBody(response)
			sresp := loadServerSetResp(body)
			keysAdded += sresp.KeysAdded
			// spread operator indicates list of arguments
			keysFailed = append(keysFailed, sresp.KeysFailed...)
		} else {
			code = partialSuccess // TODO: wrong(!) shouldn't it return failure ?
		}
		response.Body.Close()
	}
	// final check
	final := serverSetResp{KeysAdded: keysAdded, KeysFailed: keysFailed}
	if len(keysFailed) > 0 {
		code = partialSuccess
	}
	body, err := json.Marshal(final)
	if err != nil {
		return nil, serverError
	}
	return body, code
}

func sendRequestsAndMassage(reqs []*http.Request, fn massage) ([]byte, int) {
	// create wait group, channel, and response slice
	var wg sync.WaitGroup
	wg.Add(len(reqs))
	respsChan := make(chan *http.Response)
	resps := make([]*http.Response, 0)

	// shoot the requests
	for _, curReq := range reqs {
		go func(curReq *http.Request) {
			defer wg.Done()
			curReq.Header.Set("Content-Type", "application/json")
			client := &http.Client{}
			resp, err := client.Do(curReq)
			if err != nil {
				panic(err)
			} else {
				respsChan <- resp
			}
		}(curReq)
	}

	// collect responses to resps
	go func() {
		for response := range respsChan {
			resps = append(resps, response)
		}
	}()
	wg.Wait()

	// trigger massager
	return fn(resps)
}

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

func compositeServerReq(endpoint string, reqBody interface{}) *http.Request {
	jsonStr, err := json.Marshal(&reqBody)
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
	file, e := ioutil.ReadFile(serverConfig)
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

func loadServerSetResp(jsonBytes []byte) serverSetResp {
	var resps serverSetResp
	json.Unmarshal(jsonBytes, &resps)
	return resps
}

func loadServerQueryResp(jsonBytes []byte) []serverQueryResp {
	var resps []serverQueryResp
	json.Unmarshal(jsonBytes, &resps)
	return resps
}

func loadServerFetchResp(jsonBytes []byte) []serverFetchResp {
	var resps []serverFetchResp
	json.Unmarshal(jsonBytes, &resps)
	return resps
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
