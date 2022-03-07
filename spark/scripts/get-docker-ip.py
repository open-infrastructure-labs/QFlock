#!/usr/bin/python3
import sys
import subprocess
import json

if len(sys.argv) < 2:
    print("get-docker-ip.py <docker-name>")
    print("missing docker name")
    exit(1)
docker_name = sys.argv[1]
result = subprocess.run('docker network inspect qflock-net'.split(' '), stdout=subprocess.PIPE)
d = json.loads(result.stdout)

for c in d[0]['Containers'].values():
    # print(c['Name'], c['IPv4Address'].split('/')[0])
    if c['Name'] == docker_name:
        docker_ip = c['IPv4Address'].split('/')[0]
        print(docker_ip)
