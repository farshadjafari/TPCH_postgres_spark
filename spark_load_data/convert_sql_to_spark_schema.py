import pprint
import json


with open('sql_tables', 'r') as f:
    schemes = {'tables': {}}
    for l in f:
        print l
        if not l: continue
        l = l.strip()
        field_name = l.split('\t')[0]
        if 'CREATE TABLE' in l:
            table_name = l.split()[2]
            schemes['tables'][table_name] = []
        elif 'EMPTY' in l:
            continue
        elif ('SERIAL PRIMARY KEY' in l) or ('INTEGER' in l) :
            schemes['tables'][table_name].append({
                'col_name': l.split('\t')[0],
                'col_type': 'INT'
            })
            print 'StructField("%s", IntegerType(), True),' % l.split('\t')[0]
        elif 'CHAR' in l:
            schemes['tables'][table_name].append({
                'col_name': l.split('\t')[0],
                'col_type': 'STR'
            })
            print 'StructField("%s", StringType(), True),' % l.split('\t')[0]
        elif 'DECIMAL' in l:
            schemes['tables'][table_name].append({
                'col_name': l.split('\t')[0],
                'col_type': 'DEC'
            })
            print 'StructField("%s", DecimalType(), True),'
        elif 'BIGINT' in l:
            schemes['tables'][table_name].append({
                'col_name': l.split('\t')[0],
                'col_type': 'LNG'
            })
            print 'StructField("%s", LongType(), True),'
        elif 'DATE' in l:
            schemes['tables'][table_name].append({
                'col_name': l.split('\t')[0],
                'col_type': 'DAT'
            })
            print 'StructField("%s", DateType(), True),'
with open('spark_tables.json', 'w') as o:
    o.write(json.dumps(schemes))
    o.close()
    pprint.pprint(schemes)
#
# x = {"tables": {"CUSTOMER": [{"col_name": "C_CUSTKEY", "col_type": "INT"}, {"col_name": "C_NAME", "col_type": "STR"},
#                              {"col_name": "C_ADDRESS", "col_type": "STR"},
#                              {"col_name": "C_NATIONKEY", "col_type": "LNG"}, {"col_name": "C_PHONE", "col_type": "STR"},
#                              {"col_name": "C_ACCTBAL", "col_type": "DEC"},
#                              {"col_name": "C_MKTSEGMENT", "col_type": "STR"},
#                              {"col_name": "C_COMMENT", "col_type": "STR"}],
#                 "LINEITEM": [{"col_name": "L_ORDERKEY", "col_type": "LNG"},
#                              {"col_name": "L_PARTKEY", "col_type": "LNG"}, {"col_name": "L_SUPPKEY", "col_type": "LNG"},
#                              {"col_name": "L_QUANTITY", "col_type": "DEC"},
#                              {"col_name": "L_EXTENDEDPRICE", "col_type": "DEC"},
#                              {"col_name": "L_DISCOUNT", "col_type": "DEC"}, {"col_name": "L_TAX", "col_type": "DEC"},
#                              {"col_name": "L_RETURNFLAG", "col_type": "STR"},
#                              {"col_name": "L_LINESTATUS", "col_type": "STR"},
#                              {"col_name": "L_SHIPDATE", "col_type": "DAT"},
#                              {"col_name": "L_COMMITDATE", "col_type": "DAT"},
#                              {"col_name": "L_RECEIPTDATE", "col_type": "DAT"},
#                              {"col_name": "L_SHIPINSTRUCT", "col_type": "STR"},
#                              {"col_name": "L_SHIPMODE", "col_type": "STR"},
#                              {"col_name": "L_COMMENT", "col_type": "STR"}],
#                 "PARTSUPP": [{"col_name": "PS_PARTKEY", "col_type": "LNG"},
#                              {"col_name": "PS_SUPPKEY", "col_type": "LNG"},
#                              {"col_name": "PS_SUPPLYCOST", "col_type": "DEC"},
#                              {"col_name": "PS_COMMENT", "col_type": "STR"}],
#                 "REGION": [{"col_name": "R_REGIONKEY", "col_type": "INT"}, {"col_name": "R_NAME", "col_type": "STR"},
#                            {"col_name": "R_COMMENT", "col_type": "STR"}],
#                 "NATION": [{"col_name": "N_NATIONKEY", "col_type": "INT"}, {"col_name": "N_NAME", "col_type": "STR"},
#                            {"col_name": "N_REGIONKEY", "col_type": "LNG"},
#                            {"col_name": "N_COMMENT", "col_type": "STR"}],
#                 "PART": [{"col_name": "P_PARTKEY", "col_type": "INT"},
#                          {"col_name": "P_NAME", "col_type": "STR"},
#                          {"col_name": "P_MFGR", "col_type": "STR"},
#                          {"col_name": "P_BRAND", "col_type": "STR"},
#                          {"col_name": "P_TYPE", "col_type": "STR"},
#                          {"col_name": "P_SIZE", "col_type": "INT"},
#                          {"col_name": "P_CONTAINER", "col_type": "STR"},
#                          {"col_name": "P_RETAILPRICE", "col_type": "DEC"},
#                          {"col_name": "P_COMMENT", "col_type": "STR"}],
#                 "SUPPLIER": [{"col_name": "S_SUPPKEY", "col_type": "INT"}, {"col_name": "S_NAME", "col_type": "STR"},
#                              {"col_name": "S_ADDRESS", "col_type": "STR"},
#                              {"col_name": "S_NATIONKEY", "col_type": "LNG"}, {"col_name": "S_PHONE", "col_type": "STR"},
#                              {"col_name": "S_ACCTBAL", "col_type": "DEC"},
#                              {"col_name": "S_COMMENT", "col_type": "STR"}],
#                 "ORDERS": [{"col_name": "O_ORDERKEY", "col_type": "INT"}, {"col_name": "O_CUSTKEY", "col_type": "LNG"},
#                            {"col_name": "O_ORDERSTATUS", "col_type": "STR"},
#                            {"col_name": "O_TOTALPRICE", "col_type": "DEC"},
#                            {"col_name": "O_ORDERDATE", "col_type": "DAT"},
#                            {"col_name": "O_ORDERPRIORITY", "col_type": "STR"},
#                            {"col_name": "O_CLERK", "col_type": "STR"}, {"col_name": "O_COMMENT", "col_type": "STR"}]}}
