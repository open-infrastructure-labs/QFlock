import os
import time
import http.client
import json
import subprocess


def get_docker_ip(docker_name: str):
    result = subprocess.run('docker network inspect qflock-net'.split(' '), stdout=subprocess.PIPE)
    d = json.loads(result.stdout)

    for c in d[0]['Containers'].values():
        if c['Name'] == docker_name:
            addr = c['IPv4Address'].split('/')[0]
            with open('host_aliases', 'w') as f:
                f.write(f'{docker_name} {addr}')

            os.environ.putenv('HOSTALIASES', 'host_aliases')
            os.environ['HOSTALIASES'] = 'host_aliases'
            return addr

    return None


def get_bytes_read(datanode_name: str):
    conn = http.client.HTTPConnection(f'{datanode_name}:9864')
    req = f'/jmx?qry=Hadoop:service=DataNode,name=DataNodeActivity-{datanode_name}-9866'
    conn.request("GET", req)
    resp = conn.getresponse()
    resp_data = resp.read()
    d = json.loads(resp_data)
    conn.close()
    return d['beans'][0]['BytesRead']


if __name__ == '__main__':
    datanode_name = 'qflock-storage-dc1'
    storage_ip = get_docker_ip(datanode_name)

    start_bytes = get_bytes_read(datanode_name)
    while True:
        time.sleep(1)
        end_bytes = get_bytes_read(datanode_name)
        if end_bytes - start_bytes > 10 << 10:
            print(end_bytes - start_bytes)
            start_bytes = end_bytes
