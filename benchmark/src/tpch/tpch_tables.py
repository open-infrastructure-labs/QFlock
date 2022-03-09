

tpch_table_dict = {
    "customer": {'columns': ({'name': "c_custkey", 'type': "LONG"},
                             {'name': "c_name", 'type': "STRING"},
                             {'name': "c_address", 'type': "STRING"},
                             {'name': "c_nationkey", 'type': "LONG"},
                             {'name': "c_phone", 'type': "STRING"},
                             {'name': "c_acctbal", 'type': "DOUBLE"},
                             {'name': "c_mktsegment", 'type': "STRING"},
                             {'name': "c_comment", 'type': "STRING"},
                             )
                 },
    "lineitem": {'columns': ({'name': "l_orderkey", 'type': "LONG"},
                             {'name': "l_partkey", 'type': "LONG"},
                             {'name': "l_suppkey", 'type': "LONG"},
                             {'name': "l_linenumber", 'type': "LONG"},
                             {'name': "l_quantity", 'type': "DOUBLE"},
                             {'name': "l_extendedprice", 'type': "DOUBLE"},
                             {'name': "l_discount", 'type': "DOUBLE"},
                             {'name': "l_tax", 'type': "DOUBLE"},
                             {'name': "l_returnflag", 'type': "STRING"},
                             {'name': "l_linestatus", 'type': "STRING"},
                             {'name': "l_shipdate", 'type': "STRING"},
                             {'name': "l_commitdate", 'type': "STRING"},
                             {'name': "l_receiptdate", 'type': "STRING"},
                             {'name': "l_shipinstruct", 'type': "STRING"},
                             {'name': "l_shipmode", 'type': "STRING"},
                             {'name': "l_comment", 'type': "STRING"},
                             )
                 },
    "nation": {'columns': ({'name': "n_nationkey", 'type': "LONG"},
                           {'name': "n_name", 'type': "STRING"},
                           {'name': "n_regionkey", 'type': "LONG"},
                           {'name': "n_comment", 'type': "STRING"},
                           )
               },
    "orders": {'columns': ({'name': "o_orderkey", 'type': "LONG"},
                          {'name': "o_custkey", 'type': "LONG"},
                          {'name': "o_orderstatus", 'type': "STRING"},
                          {'name': "o_totalprice", 'type': "DOUBLE"},
                          {'name': "o_orderdate", 'type': "STRING"},
                          {'name': "o_orderpriority", 'type': "STRING"},
                          {'name': "o_clerk", 'type': "STRING"},
                          {'name': "o_shippriority", 'type': "LONG"},
                          {'name': "o_comment", 'type': "STRING"},
                          )
              },
    "part": {'columns': ({'name': "p_partkey", 'type': "LONG"},
                         {'name': "p_name", 'type': "STRING"},
                         {'name': "p_mfgr", 'type': "STRING"},
                         {'name': "p_brand", 'type': "STRING"},
                         {'name': "p_type", 'type': "STRING"},
                         {'name': "p_size", 'type': "LONG"},
                         {'name': "p_container", 'type': "STRING"},
                         {'name': "p_retailprice", 'type': "DOUBLE"},
                         {'name': "p_comment", 'type': "STRING"},
                         )
             },
    "partsupp": {'columns': ({'name': "ps_partkey", 'type': "LONG"},
                             {'name': "ps_suppkey", 'type': "LONG"},
                             {'name': "ps_availqty", 'type': "LONG"},
                             {'name': "ps_supplycost", 'type': "DOUBLE"},
                             {'name': "ps_comment", 'type': "STRING"},
                             )
                 },
    "region": {'columns': ({'name': "r_regionkey", 'type': "LONG"},
                           {'name': "r_name", 'type': "STRING"},
                           {'name': "r_comment", 'type': "STRING"},
                           )
               },
    "supplier": {'columns': ({'name': "s_suppkey", 'type': "LONG"},
                             {'name': "s_name", 'type': "STRING"},
                             {'name': "s_address", 'type': "STRING"},
                             {'name': "s_nationkey", 'type': "LONG"},
                             {'name': "s_phone", 'type': "STRING"},
                             {'name': "s_acctbal", 'type': "DOUBLE"},
                             {'name': "s_comment", 'type': "STRING"},
                             )
                 },
}