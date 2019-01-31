import time
import queries

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Running queries in 'parquet', 'orc', 'avro' formats") \
    .getOrCreate()

for frmt in ['parquet', 'orc', 'avro']:
    read_format = frmt
    if frmt == 'avro':
        read_format = 'com.databricks.spark.avro'
    spark.sql("set spark.sql.orc.impl=native")
    customer = spark.read.format(read_format).load("hdfs://namenode:8020/frshd/CUSTOMER.%s" % frmt)
    lineitem = spark.read.format(read_format).load("hdfs://namenode:8020/frshd/LINEITEM.%s" % frmt)
    part = spark.read.format(read_format).load("hdfs://namenode:8020/frshd/PART.%s" % frmt)
    supplier = spark.read.format(read_format).load("hdfs://namenode:8020/frshd/SUPPLIER.%s" % frmt)
    partsupp = spark.read.format(read_format).load("hdfs://namenode:8020/frshd/PARTSUPP.%s" % frmt)
    nation = spark.read.format(read_format).load("hdfs://namenode:8020/frshd/NATION.%s" % frmt)
    orders = spark.read.format(read_format).load("hdfs://namenode:8020/frshd/ORDERS.%s" % frmt)
    region = spark.read.format(read_format).load("hdfs://namenode:8020/frshd/REGION.%s" % frmt)
    tables = [customer, lineitem, part, supplier, partsupp, nation, orders, region]
    for i in range(1, 2):
        start_time = time.time()
        querie = getattr(queries, 'q%s' % i)
        try:
            result = querie(*tables, is_avro=(frmt == 'avro'))
        except Exception as e:
            print(e)
            result = e
        try:
            result.coalesce(1).write.format("csv").save('%s_%s_out.csv' % (i, frmt), header='true')
        except:
            with open('%s_%s_out.ERR' % (i, frmt), 'w') as out:
                out.write(str(result))
        print("--- %s seconds ---" % (time.time() - start_time))
        duration = time.time() - start_time
        with open('times', 'a') as times:
            times.write("--- query no %s - %s ---\n" % (i, frmt))
            times.write("%s seconds\n\n" % duration)
# spark.stop()
