import os
import time
import http.client
import json
import subprocess


def get_docker_ip(docker_name: str):
    result = subprocess.run('docker network inspect qflock-net'.split(' '), stdout=subprocess.PIPE)
    d = json.loads(result.stdout)

    f = open('host_aliases', 'w')
    for c in d[0]['Containers'].values():
        name = c['Name']
        addr = c['IPv4Address'].split('/')[0]
        f.write(f'{name} {addr}\n')

    f.close()
    os.environ.putenv('HOSTALIASES', 'host_aliases')
    os.environ['HOSTALIASES'] = 'host_aliases'

    for c in d[0]['Containers'].values():
        if c['Name'] == docker_name:
            addr = c['IPv4Address'].split('/')[0]
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

def get_network_stats(docker_name):
    # docker stats --no-stream --format "{{.NetIO}}" qflock-storage
    cmd = 'docker stats --no-stream --format "{{.NetIO}}" ' + docker_name
    result = subprocess.run(cmd.split(' '), stdout=subprocess.PIPE)
    stats = result.stdout.decode("utf-8")
    stats = stats.lower().replace('\n', '').replace('"', '').replace(' ', '').replace('b', '').split('/')
    print(stats)
    sizes = {'k': 1 << 10, 'm': 1 << 20, 'g': 1 << 30}

    for i in range(0, len(stats)):
        factor = 1
        if stats[i][-1] in sizes.keys():
            factor = sizes[stats[i][-1]]
            stats[i] = stats[i][:-1]

        stats[i] = float(stats[i]) * factor

    return stats


if __name__ == '__main__':
    datanode_name = 'qflock-storage-dc1'
    storage_ip = get_docker_ip(datanode_name)

    while True:
        time.sleep(1)
        get_network_stats(datanode_name)
