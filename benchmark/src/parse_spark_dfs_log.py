#!/usr/bin/python3
import sys

def parse(p_file):
    with open(p_file, 'r') as fd:
        total = 0
        data = 0
        metadata = 0
        for line in fd.readlines():
            if "DFSClient readNextPacket" in line:
                items = line.rstrip("\n").split(" ")
                cur_bytes = int(items[10].split("=")[1])
                total += cur_bytes
                if cur_bytes < 65535:
                    metadata += cur_bytes
                else:
                    data += cur_bytes
        metadata_pct = (metadata / total) * 100
        data_pct = (data / total) * 100
        print(f"total {total} data {data}:{data_pct:2.3f} metadata {metadata}:{metadata_pct:2.3f}")


if __name__ == "__main__":
    file = sys.argv[1]
    parse(file)
