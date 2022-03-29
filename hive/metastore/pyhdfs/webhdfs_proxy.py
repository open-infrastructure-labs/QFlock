#!/usr/bin/python3 -u
import threading
import time
import subprocess
import json
import http.server
import http.client
import urllib.parse


class ChunkedWriter:
    def __init__(self, wfile):
        self.wfile = wfile

    def write(self, data):
        self.wfile.write(f'{len(data):x}\r\n'.encode())
        self.wfile.write(data)
        self.wfile.write('\r\n'.encode())

    def close(self):
        self.wfile.write('0\r\n\r\n'.encode())


logging_lock = threading.Lock()
logging_file = None

hdfs_nn_port = 9870
hdfs_dn_port = 9864

proxy_nn_port = 9860
proxy_dn_port = 9859

hdfs_ip = 'qflock-storage'

total_bytes = 0


class NNRequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def log(self, msg):
        logging_lock.acquire()
        print(msg)
        logging_file.write(msg + '\n')
        logging_file.flush()
        logging_lock.release()

    def send_hdfs_request(self):
        conn = http.client.HTTPConnection(f'{hdfs_ip}:{hdfs_nn_port}')
        conn.request("GET", self.path, '', self.headers)
        response = conn.getresponse()
        data = response.read()
        conn.close()
        return response, data

    def do_GET(self):
        return self.forward_to_hdfs()

    def forward_to_hdfs(self):
        global total_bytes
        resp, data = self.send_hdfs_request()
        total_bytes += len(data)
        self.log(f'"NN", "{self.path}", {len(data)}, {total_bytes} ')
        self.send_response(resp.status, resp.reason)

        transfer_encoding = None
        for h in resp.headers.items():
            if h[0] == 'Transfer-Encoding':
                transfer_encoding = h[1]

            if h[0] == 'Location' and resp.status == 307:  # Temporary redirect
                ip = self.connection.getsockname()[0]
                location = h[1].replace(f':{hdfs_dn_port}', f':{proxy_dn_port}', 1)
                # print(location)
                self.send_header(h[0], location)
            else:
                self.send_header(h[0], h[1])

        self.end_headers()
        if transfer_encoding == 'chunked':
            writer = ChunkedWriter(self.wfile)
            writer.write(data)
            writer.close()
        else:
            self.wfile.write(data)

        self.wfile.flush()


class DNRequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def log(self, msg):
        logging_lock.acquire()
        print(msg)
        logging_file.write(msg + '\n')
        logging_file.flush()
        logging_lock.release()

    def send_hdfs_request(self):
        conn = http.client.HTTPConnection(f'{hdfs_ip}:{hdfs_dn_port}')
        conn.request("GET", self.path, '', self.headers)
        response = conn.getresponse()
        data = response.read()
        conn.close()
        return response, data

    def do_GET(self):
        return self.forward_to_hdfs()

    def forward_to_hdfs(self):
        global total_bytes
        resp, data = self.send_hdfs_request()
        total_bytes += len(data)
        self.log(f'"DN", "{self.path}", {len(data)}, {total_bytes} ')
        self.send_response(resp.status, resp.reason)
        transfer_encoding = None
        for h in resp.headers.items():
            if h[0] == 'Transfer-Encoding':
                transfer_encoding = h[1]

            self.send_header(h[0], h[1])

        self.end_headers()
        if transfer_encoding == 'chunked':
            writer = ChunkedWriter(self.wfile)
            writer.write(data)
            writer.close()
        else:
            self.wfile.write(data)

        self.wfile.flush()


def start_server(port, handler):
    server = http.server.HTTPServer(('0.0.0.0', port), handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass

    server.server_close()


def get_storage_ip():
    result = subprocess.run('docker network inspect qflock-net'.split(' '), stdout=subprocess.PIPE)
    d = json.loads(result.stdout)

    for c in d[0]['Containers'].values():
        print(c['Name'], c['IPv4Address'].split('/')[0])
        if c['Name'] == 'qflock-storage':
            return c['IPv4Address'].split('/')[0]

    return None


if __name__ == '__main__':
    logging_file = open('webhdfs_proxy.log', 'w')

    # hdfs_ip = get_storage_ip()

    print(f'Listening to ports:{proxy_nn_port}, {proxy_dn_port} HDFS:{hdfs_ip}')

    # start_server(proxy_nn_port, NNRequestHandler)
    th = threading.Thread(target=start_server, args=(proxy_nn_port, NNRequestHandler), daemon=True).start()
    start_server(proxy_dn_port, DNRequestHandler)

    logging_file.close()
