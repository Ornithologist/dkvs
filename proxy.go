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
	"strconv"
	"strings"
	"sync"
)

const success int = 200
const partialSuccess int = 206
const clientError int = 405
const serverError int = 500
const serverConfig string = "servers.json"

type massage func([]*http.Response) ([]byte, int)

type server struct {
	IP   string `json:"ip"`
	Port int    `json:"port"`
}

type errorResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Encoded - base struct used in all requests and responses
type Encoded struct {
	Encoding string `json:"encoding"`
	Data     string `json:"data"`
}

type serverSetResp struct {
	KeysAdded  int       `json:"keys_added"`
	KeysFailed []Encoded `json:"keys_failed"`
}

type serverFetchResp struct {
	Key   Encoded  `json:"key"`
	Value *Encoded `json:"value"`
}

type serverQueryResp struct {
	Key   Encoded `json:"key"`
	Value bool    `json:"value"`
}

type clientSetReq struct {
	Key   Encoded `json:"key"`
	Value Encoded `json:"value"`
}

type clientFetQueReq struct {
	Key Encoded `json:"key"`
}

var servers []server

func handler(w http.ResponseWriter, r *http.Request) {
	endpoint := r.URL.Path
	method := r.Method
	if method == http.MethodGet && endpoint == "/fetch" {
		handleFetchAll(w, r)
	} else if method == http.MethodPost && endpoint == "/fetch" {
		handleFetch(w, r)
	} else if method == http.MethodPost && endpoint == "/query" {
		handleQuery(w, r)
	} else if method == http.MethodPut && endpoint == "/set" {
		handleSet(w, r)
	} else {
		handleError(w, r, &errorResp{Code: clientError, Message: "Request not found."})
	}
}

func handleFetchAll(w http.ResponseWriter, r *http.Request) {
	// assemble the requests (one per server)
	reqs := make([]*http.Request, 0)
	for j := 0; j < len(servers); j++ {
		// prepare the request
		serverEndpoint := fmt.Sprintf("http://%s:%d/fetch", servers[j].IP, servers[j].Port)
		httpReq := compositeServerReq(serverEndpoint, nil, http.MethodGet)
		reqs = append(reqs, httpReq)
	}

	// send request, wait, massage, and sent response to client
	output, code := sendRequestsAndMassage(reqs, massageFetch)
	handleSuccess(w, r, output, code)
}

func handleFetch(w http.ResponseWriter, r *http.Request) {
	// extract the key value pairs
	body := loadReqBody(r)
	queryReqs := loadFetQueRequest(body)

	// massage the body
	isValid := true
	serverReqMap := make(map[int][]Encoded)
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
		serverReqMap[serverIdx] = append(serverReqMap[serverIdx], queryReqs[i].Key)
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
		httpReq := compositeServerReq(serverEndpoint, serverReqs, http.MethodPost)
		reqs = append(reqs, httpReq)
	}

	// send request, wait, massage, and sent response to client
	output, code := sendRequestsAndMassage(reqs, massageFetch)
	handleSuccess(w, r, output, code)
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	// extract the key value pairs
	body := loadReqBody(r)
	queryReqs := loadFetQueRequest(body)

	// massage the body
	isValid := true
	serverReqMap := make(map[int][]Encoded)
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
		serverReqMap[serverIdx] = append(serverReqMap[serverIdx], queryReqs[i].Key)
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
		httpReq := compositeServerReq(serverEndpoint, serverReqs, http.MethodPost)
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
	serverReqMap := make(map[int][]clientSetReq)
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

		// append server request
		serverReqMap[serverIdx] = append(serverReqMap[serverIdx], setReqs[i])
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
		httpReq := compositeServerReq(serverEndpoint, serverReqs, http.MethodPut)
		reqs = append(reqs, httpReq)
	}

	// send request, wait, massage, and sent response to client
	output, code := sendRequestsAndMassage(reqs, massageSet)
	handleSuccess(w, r, output, code)
}

/*
 * Life cycle functions
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
	keysFailed := make([]Encoded, 0)
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
	// create mutext lock, wait group, and response slice
	var mutex = &sync.Mutex{}
	var wg sync.WaitGroup
	resps := make([]*http.Response, 0)

	// shoot the requests
	wg.Add(len(reqs))
	for _, curReq := range reqs {
		go func(curReq *http.Request) {
			defer wg.Done()
			curReq.Header.Set("Content-Type", "application/json")
			client := &http.Client{}
			resp, err := client.Do(curReq)
			if err != nil {
				panic(err)
			} else {
				mutex.Lock()
				resps = append(resps, resp)
				mutex.Unlock()
			}
		}(curReq)
	}
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

func compositeServerReq(endpoint string, reqBody interface{}, method string) *http.Request {
	jsonStr, err := json.Marshal(&reqBody)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	req, httpErr := http.NewRequest(method, endpoint, bytes.NewBuffer(jsonStr))
	if httpErr != nil {
		fmt.Println(httpErr)
		return nil
	}
	return req
}

/*
 * Utility functions
 */

func binToStr(s string) (string, bool) {
	realStr := base64.StdEncoding.EncodeToString([]byte(s))
	return realStr, true
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func loadServers() {
	args := os.Args[1:]
	if len(args) == 0 {
		fmt.Println("Loading server list from file...")
		file, e := ioutil.ReadFile(serverConfig)
		if e != nil {
			fmt.Printf("File error: %v\n", e)
			os.Exit(1)
		}
		json.Unmarshal(file, &servers)
	} else {
		fmt.Println("Loading server list from command line...")
		for _, arg := range args {
			ip := strings.Split(arg, ":")[0]
			port, err := strconv.Atoi(strings.Split(arg, ":")[1])
			if err != nil {
				fmt.Printf("Port number must be numeric: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("ip: %s, port: %d\n", ip, port)
			servers = append(servers, server{IP: ip, Port: port})
		}
	}
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

func loadFetQueRequest(jsonBytes []byte) []clientFetQueReq {
	var reqs []clientFetQueReq
	json.Unmarshal(jsonBytes, &reqs)
	return reqs
}

func main() {
	loadServers()
	http.HandleFunc("/", handler)
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
