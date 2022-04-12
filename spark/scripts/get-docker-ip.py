#!/usr/bin/python3
import sys
import subprocess
import json

if len(sys.argv) < 3:
    print("Usage: get-docker-ip.py <network-name> <docker-name>")
    exit(1)

network_name = sys.argv[1]
docker_name = sys.argv[2]
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
