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
    return lineitem_filtered_revenue


def q7(customer, lineitem, part, supplier, partsupp, nation, orders, region, is_avro=False):
    germany_china = nation.filter(
        (nation['N_NAME'] == 'GERMANY') |
        (nation['N_NAME'] == 'CHINA')
    )
    ger_chi_sup = germany_china.join(supplier, supplier['S_NATIONKEY'] == germany_china['N_NATIONKEY'])
    ger_chi_sup_sel = ger_chi_sup.select(ger_chi_sup['S_SUPPKEY'], ger_chi_sup['S_NATIONKEY'].alias('SUPP_NATION'))

    ger_chi_lines = germany_china.join(
        customer,
        customer['C_NATIONKEY'] == germany_china['N_NATIONKEY']
    ).join(
        orders,
        customer['C_CUSTKEY'] == orders['O_CUSTKEY']
    ).join(
        lineitem,
        orders['O_ORDERKEY'] == lineitem['L_ORDERKEY']
    ).join(
        supplier,
        supplier['S_SUPPKEY'] == lineitem['L_SUPPKEY']
    ).join(
        ger_chi_sup_sel,
        supplier['S_SUPPKEY'] == ger_chi_sup_sel['S_SUPPKEY']
    ).select(
        germany_china['N_NATIONKEY'].alias('CUST_NATION'),
        ger_chi_sup_sel['S_SUPPKEY'].alias('SUPP_NATION'),
        F.year(orders['L_SHIPDATE']).alias('L_YEAR'),
        (lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])).alias('VOLUME')
    ).filter(
        (
                ((ger_chi_sup_sel['SUPP_NATION'] == 'GERMANY') &
                 (germany_china['CUST_NATION'] == 'CHINA')) |
                ((ger_chi_sup_sel['SUPP_NATION'] == 'CHINA') &
                 (germany_china['CUST_NATION'] == 'GERMANY'))
        ) & (
            orders['L_SHIPDATE'].between(
                get_datetime(datetime.datetime(1995, 1, 1), is_avro),
                get_datetime(datetime.datetime(1996, 12, 31), is_avro),
            )
        )

    )
    ger_chi_lines_gourps = ger_chi_lines.groupBy('SUPP_NATION', 'CUST_NATION', 'L_YEAR')
    ger_chi_lines_gourps_rev = ger_chi_lines_gourps.agg(
        F.sum(ger_chi_lines_gourps['VOLUME']).alias('REVENUE')
    )
    ger_chi_lines_gourps_rev_sorted = ger_chi_lines_gourps_rev.sort(
        ger_chi_lines_gourps_rev['SUPP_NATION'],
        ger_chi_lines_gourps_rev['CUST_NATION'],
        ger_chi_lines_gourps_rev['L_YEAR'],
    )
    return ger_chi_lines_gourps_rev_sorted


def q8(customer, lineitem, part, supplier, partsupp, nation, orders, region, is_avro=False):
    asia_nations = nation.join(
        region,
        [region['R_NAME'] == 'ASIA',
         region['R_REGIONKEY'] == nation['N_REGIONKEY']]
    )
    all_nations = customer.join(
        asia_nations,
        customer['C_NATIONKEY'] == asia_nations['N_NATIONKEY']
    ).join(
        orders,
        orders['O_CUSTKEY'] == customer['C_CUSTKEY']
    ).join(
        lineitem,
        lineitem['L_ORDERKEY'] == orders['O_ORDERKEY']
    ).join(
        part,
        [part['P_TYPE'] == 'STANDARD PLATED COPPER',
         part['P_PARTKEY'] == lineitem['L_PARTKEY']]
    ).join(
        supplier.join(
            nation,
            nation['N_NATIONKEY'] == supplier['S_NATIONKEY']
        ).select(
            supplier['S_SUPPKEY'],
            nation['N_NAME'].alias('NATION')
        ),
        supplier['S_SUPPKEY'] == lineitem['L_SUPPKEY']
    ).filter(
        orders['O_ORDERDATE'].between(
            get_datetime(datetime.datetime(1995, 1, 1), is_avro),
            get_datetime(datetime.datetime(1996, 12, 31), is_avro),
        )
    ).goupBy(
        orders['O_YEAR']
    ).sort(
        orders['O_YEAR']
    ).select(
        F.year(orders['O_ORDERDATE']).alias('O_YEAR'),
        (lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])).alias('VOLUME'),
        'NATION'
    )
    china_share = all_nations.groupBy('O_YEAR').agg(
        (F.sum(all_nations['VOLUME'] if all_nations['NATION'] == 'CHINA' else 0) / F.sum(all_nations['VOLUME'])
         ).alias('MKT_SHARE')
    ).sort(
        all_nations['O_YEAR']
    )
    return china_share


def q9(customer, lineitem, part, supplier, partsupp, nation, orders, region, is_avro=False):
    df = part.filter(
        part['P_NAME'].like('%BLUE%')
    ).join(
        lineitem,
        lineitem['L_PARTKEY'] == part['P_PARTKEY']
    ).join(
        supplier,
        supplier['S_SUPPKEY'] == lineitem['L_SUPPKEY']
    ).join(
        nation,
        supplier['S_NATIONKEY'] == nation['N_NATIONKEY']
    ).join(
        orders,
        orders['O_ORDERKEY'] == lineitem['L_ORDERKEY']
    ).join(
        partsupp,
        [partsupp['PS_SUPPKEY'] == lineitem['L_SUPPKEY'],
         partsupp['PS_PARTKEY'] == lineitem['L_PARTKEY']]
    ).select(
        nation['N_NAME'].alias('NATION'),
        F.year(orders['O_ORDERDATE']).alias('O_YEAR'),
        (lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT']) - partsupp['PS_SUPPLYCOST'] * lineitem['L_QUANTITY']
         ).alias('AMOUNT'),
    ).groupBy(
        'NATION',
        orders['O_YEAR']
    ).sort(
        'NATION',
        orders['O_YEAR'].desc()
    )
    return df


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
