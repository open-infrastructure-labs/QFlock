#!/usr/bin/python3
import sys

def parse(p_file):
    with open(p_file, 'r') as fd:
        storage_totals = {}
        spark_totals = {}
        for line in fd.readlines():
            if "IP 172.18.0.2" in line or "IP qflock-storage" in line:
                date_addr = line.split(',')[0]
                ts = date_addr.split(' ')[0]
                storage_port = date_addr.split(' ')[2].split('.')[1]
                spark_port = date_addr.split(' ')[4].split('.')[1].replace(":", "")
                for item in line.split(','):
                    if "length" in item:
                        cur_bytes = int(item.strip().split(' ')[1])
                        if cur_bytes > 100:
                            print(f"{ts},{storage_port},{spark_port},{cur_bytes}")
                if storage_port not in storage_totals:
                    storage_totals[storage_port] = 0
                storage_totals[storage_port] += cur_bytes
                if spark_port not in spark_totals:
                    spark_totals[spark_port] = 0
                spark_totals[spark_port] += cur_bytes
        total = 0
        for port in storage_totals:
            print(f"{port}:{storage_totals[port]}")
            total += storage_totals[port]
        print(f"storage total: {total}")
        total = 0
        for port in spark_totals:
            print(f"{port}:{spark_totals[port]}")
            total += spark_totals[port]
        print(f"spark total: {total}")


if __name__ == "__main__":
    file = sys.argv[1]
    parse(file)
