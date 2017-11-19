# dkvs

dkvs is a distributed key-value store implemented using Go and Python. Key value pairs are distributed across multiple nodes (called servers) by the proxy.

## Author
Rishab Khandelwal - khandelwal.r@husky.neu.edu
Rongxuan Liu - liu.ron@husky.neu.edu

## Build & Run

To build, do
```make proxy``` to make the proxy.

Ideally you will need Go version 1.9.2 and above to run this proxy server. If proxy is returnning error please upgrade your Go version to latest.

To run the proxy, do
```./proxy``` to start the proxy using the pre-installed _servers.json_ configuration file.
Or, if you want to specify specific IPs and Ports, do
```./proxy <ip:port> <ip:port> ...```.
The proxy will run on port 8080.

To run the server, do
```python server.py <port>```.

## Design Overview

Two types of encodings are supported by dkvs -- binary and string. Typically, a key or value can be represented by the following JSON format: 
```
{
    "encoding": "string",
    "data": "foo"
}
```

Proxy distributes the key-value pairs by taking the modulo of the key data against the number of servers available to ensure there are relatively equal number of key-value pairs on each server.

## Interfaces

dkvs implements a RESTful API interface for fetching, setting, and querying.

### GET fetch

By making request to the http://localhost/fetch endpoint using GET, the client can retrieve *all* key-value pairs stored in the servers.

The endpoint returns a JSON list in the following format:
```json
[
    {
        "key": {
            "encoding": "string",
            "data": "foo"
        },
        "value": {
            "encoding": "string",
            "data": "bar"
        }
    },
    ...
]
```

### POST fetch

Similar to the [GET fetch](### GET fetch) endpoint, POST fetch returns a list of key-value pairs. It takes in a JSON list of keys from the request body, and returns corresponding values. The request body should follow the format below:
```json
[
    {
        "key": {
            "encoding": "string",
            "data": "foo"
        },
    },
    ...
]
```

For none existing keys, the "value" field in the response would be null. Were any key encoding specified in the request is not "string" nor "binary", the whole request is rejected.

### POST query

POST query has the same request interface as [POST fetch](### POST fetch):
```json
[
    {
        "key": {
            "encoding": "string",
            "data": "foo"
        },
    },
    ...
]
```

It returns true for the key that exists in the store, and false for the key that does not exist.

Sample response:
```json
[
    {
        "key": {
            "encoding": "string",
            "data": "foo"
        },
        "value": true
    },
    {
        "key": {
            "encoding": "string",
            "data": "bar"
        },
        "value": false
    },
]
```

### PUT set

PUT set allows the client to send a list of key-value pairs and dkvs will insert/update them accordingly. The request JSON body should follow the same format as the response of [POST fetch](### POST fetch) endpoint:

```json
[
    {
        "key": {
            "encoding": "string",
            "data": "foo"
        },
        "value": {
            "encoding": "string",
            "data": "bar"
        }
    },
    ...
]
```

The endpoind returns the number of keys that are successfully inserted/updated, and the keys that failed to update.

Sample response:
```json
[
    {
        "keys_added": 10,
        "keys_failed": [
            {
                "encoding": "string",
                "data": "bar"
            },
            {
                "encoding": "string",
                "data": "foo2"
            }
        ]
    }
]
```
