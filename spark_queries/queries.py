import sys
import time
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql import functions as F


def get_datetime(pydate, is_avro):
    if is_avro:
        return (pydate - datetime.datetime(1970, 1, 1)).total_seconds() * 1000
    return pydate


# --- query no 1 - parquet ---
# 957.2419791221619 seconds
#
# --- query no 1 - orc ---
# 1025.3170006275177 seconds
#
# --- query no 1 - avro ---
# 1242.639072418213 seconds
#

def q1(customer, lineitem, part, supplier, partsupp, nation, orders, region, is_avro=False):
    df = lineitem.filter(lineitem['L_SHIPDATE'] <= (
        get_datetime(datetime.datetime(1998, 12, 1) - datetime.timedelta(days=68), is_avro))
                         ).groupBy('L_RETURNFLAG', 'L_LINESTATUS') \
        .agg(F.sum('L_QUANTITY').alias('sum_qty'),
             F.sum('L_EXTENDEDPRICE').alias('sum_base_price'),
             F.sum(lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])).alias('sum_disc_price'),
             F.sum(lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT']) * (1 + lineitem['l_tax'])) \
             .alias('sum_charge'),
             F.avg('L_QUANTITY').alias('avg_qty'),
             F.avg('L_EXTENDEDPRICE').alias('avg_price'),
             F.avg('L_DISCOUNT').alias('avg_disc'),
             F.count('*').alias('count_order'),
             )
    print('###################################')
    # df.show()
    return df


def q2(customer, lineitem, part, supplier, partsupp, nation, orders, region, is_avro=False):
    africa = region.filter(region['R_NAME'] == 'AFRICA')
    africa_nations = nation.join(africa, nation['N_REGIONKEY'] == africa['R_REGIONKEY'])
    africa_nations_supps = supplier.join(africa_nations, supplier['S_NATIONKEY'] == africa_nations['N_NATIONKEY'])
    africa_nations_supps_partsupp = partsupp.join(africa_nations_supps,
                                                  africa_nations_supps['S_SUPPKEY'] == partsupp['PS_SUPPKEY'])
    africa_nations_supps_partsupp_part = part.join(africa_nations_supps_partsupp,
                                                   africa_nations_supps_partsupp['PS_PARTKEY'] == part['P_PARTKEY'])
    minimum_suppcost = \
        africa_nations_supps_partsupp.agg(F.min(africa_nations_supps_partsupp['PS_SUPPLYCOST'])).collect()[0][0]

    africa_nations_supps_partsupp_part_filtered = africa_nations_supps_partsupp_part.filter(
        (africa_nations_supps_partsupp_part['P_SIZE'] == 25) &
        (africa_nations_supps_partsupp_part['P_TYPE'].like('%COPPER')) &
        (africa_nations_supps_partsupp_part['PS_SUPPLYCOST'] == minimum_suppcost)
    )

    africa_nations_supps_partsupp_part_filtered_selected = africa_nations_supps_partsupp_part_filtered.select(
        africa_nations_supps_partsupp_part_filtered['S_ACCTBAL'],
        africa_nations_supps_partsupp_part_filtered['S_NAME'],
        africa_nations_supps_partsupp_part_filtered['N_NAME'],
        africa_nations_supps_partsupp_part_filtered['P_PARTKEY'],
        africa_nations_supps_partsupp_part_filtered['P_MFGR'],
        africa_nations_supps_partsupp_part_filtered['S_ADDRESS'],
        africa_nations_supps_partsupp_part_filtered['S_PHONE'],
        africa_nations_supps_partsupp_part_filtered['S_COMMENT'],
    )
    africa_nations_supps_partsupp_part_filtered_selected_limited = africa_nations_supps_partsupp_part_filtered_selected.limit(
        100)
    africa_nations_supps_partsupp_part_filtered_selected_limited_sorted = africa_nations_supps_partsupp_part_filtered_selected_limited.sort(
        africa_nations_supps_partsupp_part_filtered['S_ACCTBAL'].desc(),
        africa_nations_supps_partsupp_part_filtered['N_NAME'],
        africa_nations_supps_partsupp_part_filtered['S_NAME'],
        africa_nations_supps_partsupp_part_filtered['P_PARTKEY'],
    )
    print('###################################')
    # africa_nations_supps_partsupp_part_filtered_selected_limited_sorted.show()
    return africa_nations_supps_partsupp_part_filtered_selected_limited_sorted


def q3(customer, lineitem, part, supplier, partsupp, nation, orders, region, is_avro=False):
    housholds = customer.filter(customer['C_MKTSEGMENT'] == 'HOUSEHOLD')
    housholds_orders = housholds.join(orders, housholds['C_CUSTKEY'] == orders['O_CUSTKEY'])
    housholds_orders_filtered = housholds_orders.filter(
        housholds_orders['O_ORDERDATE'] < (
            get_datetime(datetime.datetime(1995, 3, 2), is_avro)))
    housholds_orders_filtered_lines = housholds_orders_filtered.join(
        lineitem, [lineitem['L_ORDERKEY'] == housholds_orders_filtered['O_ORDERKEY'],
                   lineitem['L_SHIPDATE'] < get_datetime(datetime.datetime(1995, 3, 2), is_avro)
                   ])
    housholds_orders_filtered_lines_grouped = housholds_orders_filtered_lines.groupBy('L_ORDERKEY', 'O_ORDERDATE',
                                                                                      'O_SHIPPRIORITY').agg(
        F.sum(
            housholds_orders_filtered_lines['L_EXTENDEDPRICE'] * (1 - housholds_orders_filtered_lines['L_DISCOUNT'])
        ).alias('REVENUE')
    )
    housholds_orders_filtered_lines_grouped_sorted = housholds_orders_filtered_lines_grouped.sort(
        housholds_orders_filtered_lines_grouped['REVENUE'].desc(),
        housholds_orders_filtered_lines_grouped['O_ORDERDATE'],
    )
    return housholds_orders_filtered_lines_grouped_sorted.limit(10)


def q4(customer, lineitem, part, supplier, partsupp, nation, orders, region, is_avro=False):
    orders_filtered = orders.filter(orders['O_ORDERDATE'].between(
        get_datetime(datetime.datetime(1993, 4, 1), is_avro),
        get_datetime(datetime.datetime(1993, 7, 1), is_avro)))
    orders_filtered_joined = orders_filtered.join(lineitem, [orders_filtered['O_ORDERKEY'] == lineitem['L_ORDERKEY'],
                                                             lineitem['L_COMMITDATE'] < lineitem['L_RECEIPTDATE']],
                                                  'leftsemi')
    orders_filtered_joined_grouped = orders_filtered_joined.groupBy('O_ORDERPRIORITY').agg(
        F.count('*').alias('order_count'))
    orders_filtered_joined_grouped_sorted = orders_filtered_joined_grouped.sort(
        orders_filtered_joined_grouped['O_ORDERPRIORITY']
    )
    return orders_filtered_joined_grouped_sorted


def q5(customer, lineitem, part, supplier, partsupp, nation, orders, region, is_avro=False):
    america = region.filter(region['R_NAME'] == 'AMERICA')
    america_nations = nation.join(
        america,
        nation['N_REGIONKEY'] == america['R_REGIONKEY']
    )
    america_nations_customers = america_nations.join(
        customer,
        america_nations['N_NATIONKEY'] == customer['C_NATIONKEY']
    )
    america_nations_suppliers = supplier.join(
        america_nations.select('N_NATIONKEY'),
        america_nations['N_NATIONKEY'] == supplier['S_NATIONKEY'],
    )
    america_nations_customers_orders = america_nations_customers.join(
        orders,
        america_nations_customers['C_CUSTKEY'] == orders['O_CUSTKEY'],
    )
    america_nations_customers_orders_filtered = america_nations_customers_orders.filter(
        america_nations_customers_orders['O_ORDERDATE'].between(
            get_datetime(datetime.datetime(1993, 1, 1), is_avro),
            get_datetime(datetime.datetime(1994, 1, 1), is_avro)
        )
    )
    america_nations_customers_orders_filtered_lineitems = america_nations_customers_orders_filtered.join(
        lineitem,
        america_nations_customers_orders_filtered['O_ORDERKEY'] == lineitem['L_ORDERKEY']
    )
    america_nations_customers_orders_filtered_lineitems_suppliers = america_nations_customers_orders_filtered_lineitems.join(
        america_nations_suppliers,
        america_nations_customers_orders_filtered_lineitems['L_SUPPKEY'] == america_nations_suppliers['S_SUPPKEY']
    )
    america_nations_customers_orders_filtered_lineitems_suppliers_grouped = america_nations_customers_orders_filtered_lineitems_suppliers.groupBy(
        america_nations_customers_orders_filtered_lineitems_suppliers['N_NAME']
    ).agg(
        F.sum(
            america_nations_customers_orders_filtered_lineitems_suppliers['L_EXTENDEDPRICE'] * (
                    1 - america_nations_customers_orders_filtered_lineitems_suppliers['L_DISCOUNT'])
        ).alias('REVENUE')
    )
    america_nations_customers_orders_filtered_lineitems_suppliers_grouped_sorted = america_nations_customers_orders_filtered_lineitems_suppliers_grouped.sort(
        america_nations_customers_orders_filtered_lineitems_suppliers_grouped['REVENUE'].desc()
    )
    return america_nations_customers_orders_filtered_lineitems_suppliers_grouped_sorted


def q6(customer, lineitem, part, supplier, partsupp, nation, orders, region, is_avro=False):
    lineitem_filtered = lineitem.filter(
        (lineitem['L_SHIPDATE'].between(
            get_datetime(datetime.datetime(1993, 1, 1), is_avro),
            get_datetime(datetime.datetime(1994, 1, 1), is_avro))) &
        (lineitem['L_DISCOUNT'].between(
            0.03, 0.05)) &
        (lineitem['L_QUANTITY'] < 25)
    )
    lineitem_filtered_revenue = lineitem_filtered.agg(
        F.sum(lineitem['L_EXTENDEDPRICE'] * lineitem['L_DISCOUNT']).alias('REVENUE')
    )
    print(lineitem_filtered.count())
    print(lineitem_filtered_revenue.collect())
    return lineitem_filtered_revenue


def q7(customer, lineitem, part, supplier, partsupp, nation, orders, region, is_avro=False):
    germany = nation.filter(nation['N_NAME'] == 'GERMANY')
    china = nation.filter(nation['N_NAME'] == 'CHINA')

    return 7


def q8(customer, lineitem, part, supplier, partsupp, nation, orders, region):
    return 8


def q9(customer, lineitem, part, supplier, partsupp, nation, orders, region):
    return 9


def q10(customer, lineitem, part, supplier, partsupp, nation, orders, region):
    return 10


def q11(customer, lineitem, part, supplier, partsupp, nation, orders, region):
    return 11


def q12(customer, lineitem, part, supplier, partsupp, nation, orders, region):
    return 12


def q13(customer, lineitem, part, supplier, partsupp, nation, orders, region):
    return 13


def q14(customer, lineitem, part, supplier, partsupp, nation, orders, region):
    return 14


def q15(customer, lineitem, part, supplier, partsupp, nation, orders, region):
    return 15


def q16(customer, lineitem, part, supplier, partsupp, nation, orders, region):
    return 16


def q17(customer, lineitem, part, supplier, partsupp, nation, orders, region):
    return 17


def q18(customer, lineitem, part, supplier, partsupp, nation, orders, region):
    return 18


def q19(customer, lineitem, part, supplier, partsupp, nation, orders, region):
    return 19


def q20(customer, lineitem, part, supplier, partsupp, nation, orders, region):
    return 20


def q21(customer, lineitem, part, supplier, partsupp, nation, orders, region):
    return 21


def q22(customer, lineitem, part, supplier, partsupp, nation, orders, region):
    return 22