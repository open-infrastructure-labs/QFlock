#!/usr/bin/python3
import sys
import subprocess
import json

if len(sys.argv) < 2:
    print("get-docker-ip.py <docker-name>")
    print("missing docker name")
    exit(1)

network_name = 'qflock-net'
docker_name = sys.argv[1]
result = subprocess.run(f'docker network inspect {network_name}'.split(' '), stdout=subprocess.PIPE)
d = json.loads(result.stdout)

if docker_name == network_name:
    print(d[0]['IPAM']['Config'][0]['Gateway'])
else:
    for c in d[0]['Containers'].values():
        # print(c['Name'], c['IPv4Address'].split('/')[0])
        if c['Name'] == docker_name:
            docker_ip = c['IPv4Address'].split('/')[0]
            print(docker_ip)
