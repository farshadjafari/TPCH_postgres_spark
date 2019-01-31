from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, LongType, DecimalType
import json


def read_json_tables():
    with open('spark_tables.json') as f:
        return json.loads(f.read())


types = {
    'INT': IntegerType(),
    'STR': StringType(),
    'DAT': DateType(),
    'LNG': LongType(),
    'DEC': DecimalType(10,2),
}

if __name__ == "__main__":
    """
        Usage: given a json of table scheme can produce dataframe equivalents in spark and load data 
        to hdfs
    """
    spark = SparkSession \
        .builder \
        .appName("SparkParquet-frshd") \
        .getOrCreate()
    tables = read_json_tables()
    for table_name, table_cols in tables['tables'].items():
        structs = []
        for table_col in table_cols:
            structs.append(StructField(table_col['col_name'], types[table_col['col_type']], True))
            print(table_col['col_name'])
            print(table_col['col_type'])
        schema = StructType(structs)
        print('reading csv...')
        df = spark.read.csv("/data/OLAP_Benchmark_data/%s.tbl" % table_name.lower(), sep="|", header=False,
                        schema=schema)
        print('writing to hdfs...')
        try:
            df.write.parquet("hdfs://namenode:8020/frshd/%s.parquet" % table_name, mode="overwrite")
        except Exception as e:
            print(e)


    spark.stop()
