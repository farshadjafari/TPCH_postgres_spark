from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, LongType, DecimalType
import json


def read_json_tables():
    with open('orc-tables.json') as f:
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
        print('reading parquet...')
        df = spark.read.parquet("hdfs://namenode:8020/frshd/%s.parquet" % table_name)
        print('writing as avro...')
        try:
            df.write.format('com.databricks.spark.avro').save("hdfs://namenode:8020/frshd/%s.avro" % table_name, mode="overwrite")
        except Exception as e:
            print(e)

    spark.stop()


