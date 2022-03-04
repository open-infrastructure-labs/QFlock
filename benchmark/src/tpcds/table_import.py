
_data_type = {"int": "LONG", "long": "LONG", "decimal": "DECIMAL", "string": "STRING", "date": "STRING"}


def table_import(file_name):
    """import a file of definitions of tpcds tables."""
    with open(file_name, "r") as fd:
        tables = {}
        table_name = ""
        for line in fd:
            #print(line)
            line = line.rstrip("\n")
            if "table." in line:
                items = line.split(".")
                table_name = items[1]
                tables[table_name] = {"columns": []}
                print(f"found table {table_name}")
            else:
                items = line.split(".")
                name = items[0]
                data_type = _data_type[items[1]]
                tables[table_name]["columns"].append({"name": name, "type": data_type})
                print(f"found column {name}: {data_type}")
    return tables


if __name__ == "__main__":
    tbl = table_import("tables.txt")

    import pprint

    pp = pprint.PrettyPrinter(indent=4)
    with open("tpcds_tables_imported.py", "w") as fd:
        print(f"_tpcds_tables = {pprint.pformat(tbl, indent=4, width=80)}", file=fd)
