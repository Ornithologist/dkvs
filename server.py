from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import SocketServer
import json
import cgi


class Store:
    '''
    Store key and value pairs in a hashmap.
    TODO: Periodically dumps the hashmap into a file.
    TODO: Read the file and load its content while booting up.abs
    '''

    def __init__(self, file_path):
        self.f = file_path
        self.store = {
            "binary": {},
            "string": {}
        }  # "binary" stores all values of binary encoded key

    def get_value_by_key(self, key):
        ''' returns the value identified by key '''
        print key["encoding"]
        if key["encoding"] not in ("binary", "string"):
            raise TypeError("Encoding must be either binary or string")
        if key["data"] in self.store[key["encoding"]]:
            return self.store[key["encoding"]][key["data"]]
        return None

    def get_all(self):
        ''' returns all key value pairs '''
        output = []
        for kyt in self.store:
            for key, value in self.store[kyt].iteritems():
                cur = {
                    "key": {
                        "encoding": kyt,
                        "data": key
                    },
                    "value": value
                }
                output.append(cur)
        return output

    def set_value_by_key(self, key, value):
        ''' sets the value using key, return success '''
        if key["encoding"] not in ("binary", "string"):
            raise TypeError("Encoding must be either binary or string")
        self.store[key["encoding"]][key["data"]] = value
        return True

    def search_by_key(self, key):
        ''' checks if the key is in the store '''
        print key
        if key["encoding"] not in ("binary", "string"):
            raise TypeError("Encoding must be either binary or string")
        return key["data"] in self.store[key["encoding"]]


class MyRequestHandler(BaseHTTPRequestHandler):
    '''
    Handles PUT, POST, and GET requests.
    Returns HTTP responses only in type application/json.
    TODO: communicates with other server nodes while bootting or halting
    '''

    def __init__(self, store, *args):
        self.store = store
        BaseHTTPRequestHandler.__init__(self, *args)

    def _set_headers(self, code=200):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def _do_set(self, req_body):
        ''' loop through the body, load key and value pairs to store '''
        # TODO: pre-check KeyError before doing set
        code = 200
        failed_keys = []
        count = 0
        try:
            for kvp in req_body:
                result = self.store.set_value_by_key(kvp["key"], kvp["value"])
                if not result:
                    failed_keys.append(kvp["key"])
                else:
                    count += 1
        except TypeError:
            return {"error": True, "message": "Bad request"}, 400
        except KeyError:
            return {"error": True, "message": "Bad request"}, 400
        if count < len(req_body):
            code = 206
        return {"keys_added": count, "keys_failed": failed_keys}, code

    def _do_fetch(self, req_body):
        ''' loop through the body, get each value by key '''
        output = [
            {
                "key": key,
                "value": self.store.get_value_by_key(key)
            }
            for key in req_body
        ]
        code = 200
        for kvp in output:
            if kvp["value"] is None:
                code = 206
        return output, code

    def _do_fetch_all(self):
        ''' return all key value pairs in the store '''
        code = 200
        output = self.store.get_all()
        return output, code

    def _do_query(self, req_body):
        ''' loop through the body, check key existence '''
        output = [
            {
                "key": key,
                "value": self.store.search_by_key(key)
            }
            for key in req_body
        ]
        code = 200
        for kvp in output:
            if not kvp["value"]:
                code = 206
        return output, code

    def do_HEAD(self):
        self._set_headers()

    # handles all GET endpoints
    def do_GET(self):
        code = 404
        message = None
        if self.path == '/fetch':
            message, code = self._do_fetch_all()
        # send response
        self._set_headers(code)
        self.wfile.write(json.dumps(message))

    # handles all PUT endpoints
    def do_PUT(self):
        ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
        code = 200
        message = None

        # refuse to receive non-json content
        if ctype != 'application/json':
            self.send_response(400)
            self.end_headers()
            return

        # read the req_body and convert it into a dictionary
        length = int(self.headers.getheader('content-length'))
        req_body = json.loads(self.rfile.read(length))

        # handle different endpoints
        if self.path == '/set':
            message, code = self._do_set(req_body)
        elif self.path == '/fetch':
            message, code = self._do_fetch(req_body)
        elif self.path == '/query':
            message, code = self._do_query(req_body)

        # send response
        self._set_headers(code)
        self.wfile.write(json.dumps(message))

    # handles all POST endpoints
    def do_POST(self):
        ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
        code = 200
        message = None

        # refuse to receive non-json content
        if ctype != 'application/json':
            self.send_response(400)
            self.end_headers()
            return

        # read the req_body and convert it into a dictionary
        length = int(self.headers.getheader('content-length'))
        req_body = json.loads(self.rfile.read(length))

        # handle different endpoints
        if self.path == '/set':
            message, code = self._do_set(req_body)
        elif self.path == '/fetch':
            message, code = self._do_fetch(req_body)
        elif self.path == '/query':
            message, code = self._do_query(req_body)

        # send response
        self._set_headers(code)
        self.wfile.write(json.dumps(message))


class MyServer:
    ''' boots HTTPServer '''

    def __init__(self, store, port=8080):
        def handler(*args):
            ''' wraps MyRequestHandler '''
            MyRequestHandler(store, *args)
        server = HTTPServer(('', port), handler)
        print 'Starting httpd on port %d...' % port
        server.serve_forever()


def run(server_class=MyServer, store_class=Store, port=8080):
    store = store_class('')
    server = server_class(store, port=port)


if __name__ == "__main__":
    from sys import argv

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()
