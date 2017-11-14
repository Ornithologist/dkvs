from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import SocketServer
import simplejson
import cgi

class Store:
    '''
    Store key and value pairs in a hashmap.
    TODO: Periodically dumps the hashmap into a file.
    TODO: Read the file and load its content while booting up.abs
    '''

    def __init__(self, f):
        self.f = f
        self.store = {}

    def get_value_by_key(self, key):
        ''' returns the value identified by key '''
        if key in self.store:
            return self.store[key]
        return None

    def set_value_by_key(self, key, value):
        ''' sets the value using key '''
        self.store[key] = value

    def search_by_key(self, key):
        ''' checks if the key is in the store '''
        return key in self.store


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

    @staticmethod
    def _do_fetch(req_body):
        return {"code": 400, "message": "Bad request"}, 400

    @staticmethod
    def _do_set(req_body):
        return {"code": 400, "message": "Bad request"}, 400

    @staticmethod
    def _do_query(req_body):
        return {"code": 400, "message": "Bad request"}, 400

    def do_HEAD(self):
        self._set_headers()

    # handles all GET endpoints
    def do_GET(self):
        self._set_headers()
        self.wfile.write(simplejson.dumps([{'key': '1001', 'value': 'test'}]))

    # handles all PUT endpoints
    def do_PUT(self):
        self._set_headers()
        self.wfile.write(simplejson.dumps([{'key': '1001', 'value': 'test'}]))

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
        req_body = simplejson.loads(self.rfile.read(length))

        # handle different endpoints
        if self.path == '/set':
            message, code = self._do_set(req_body)
        elif self.path == '/fetch':
            message, code = self._do_fetch(req_body)
        elif self.path == '/query':
            message, code = self._do_query(req_body)

        # send response
        self._set_headers(code)
        self.wfile.write(simplejson.dumps(message))


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
