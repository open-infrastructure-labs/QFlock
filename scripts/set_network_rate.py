import subprocess
import json
import sys

def get_veth(interface: str):
    result = subprocess.run(f'ip addr show master {interface}'.split(' '), stdout=subprocess.PIPE)
    output = result.stdout.decode("utf-8")

    veth = list()
    for line in output.splitlines():
        if f'master {interface}' in line:
            # print(line.split(' ')[1].split('@')[0])
            veth.append(line.split(' ')[1].split('@')[0])

    return veth

def set_rate(veth:str, rate:str):
    sep = '\n\t'
    # Check if we need to delete TBF (Token Bucket Filter)
    cmd = f'sudo tc qdisc show dev {veth}'
    result = subprocess.run(cmd.split(' '), stdout=subprocess.PIPE)
    if 'qdisc tbf' in result.stdout.decode("utf-8"):
        # Delete old TBF
        cmd = f'sudo tc qdisc del dev {veth} root '
        result = subprocess.run(cmd.split(' '), stdout=subprocess.PIPE)
        print(cmd, result.stdout.decode("utf-8"))

    # Add new TBF
    cmd = f'sudo tc qdisc add dev {veth} root tbf rate {rate} limit 128mb burst 128kb'
    result = subprocess.run(cmd.split(' '), stdout=subprocess.PIPE)
    print(cmd, result.stdout.decode("utf-8"))

    cmd = f'sudo tc qdisc show dev {veth}'
    result = subprocess.run(cmd.split(' '), stdout=subprocess.PIPE)
    print(cmd, result.stdout.decode("utf-8"), sep=sep)

    # sudo tc qdisc add dev veth938e1c6 root tbf rate 1gbit limit 128mb burst 128kb
    # sudo tc qdisc show dev veth938e1c6
    # sudo tc qdisc del dev veth938e1c6 root

if __name__ == '__main__':
    result = subprocess.run('docker network ls'.split(' '), stdout=subprocess.PIPE)
    output = result.stdout.decode("utf-8")
    # print(output)
    interface = None
    for line in output.splitlines():
        if sys.argv[1]+' ' in line:
            id = line.split(' ')[0]
            interface = f'br-{id}'
            print(interface)

    veth = get_veth(interface)
    print(veth)
    for v in veth:
        set_rate(v, sys.argv[2])

# docker exec -it qflock-storage-dc1 iperf3 -s
# docker exec -it qflock-storage-dc2 iperf3 -s
# docker exec -it qflock-spark-dc1 iperf3 -c qflock-storage-dc1
#    [  5]   0.00-1.00   sec   117 MBytes   985 Mbits/sec    0   3.03 MBytes
#    [  5]   1.00-2.00   sec   114 MBytes   954 Mbits/sec    0   3.03 MBytes
# docker exec -it qflock-spark-dc1 iperf3 -c qflock-storage-dc1
#    [  5]   1.00-2.00   sec  11.2 MBytes  94.4 Mbits/sec    0   1.28 MBytes
#    [  5]   2.00-3.00   sec  11.2 MBytes  94.4 Mbits/sec    0   1.84 MBytes
#    [  5]   3.00-4.00   sec  11.2 MBytes  94.4 Mbits/sec    0   2.42 MBytes

