-- USING 1544205479 AS A SEED TO THE RNG


BEGIN;
\O 3.SQL.OUT
\TIMING ON
SELECT
	L_ORDERKEY,
	SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
	O_ORDERDATE,
	O_SHIPPRIORITY
FROM
	CUSTOMER,
	ORDERS,
	LINEITEM
WHERE
	C_MKTSEGMENT = 'HOUSEHOLD'
	AND C_CUSTKEY = O_CUSTKEY
	AND L_ORDERKEY = O_ORDERKEY
	AND O_ORDERDATE < DATE '1995-03-02'
	AND L_SHIPDATE > DATE '1995-03-02'
GROUP BY
	L_ORDERKEY,
	O_ORDERDATE,
	O_SHIPPRIORITY
ORDER BY
	REVENUE DESC,
	O_ORDERDATE
LIMIT 10;
COMMIT;
