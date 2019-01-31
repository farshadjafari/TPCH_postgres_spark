-- USING 1544205479 AS A SEED TO THE RNG


BEGIN;
\O 4.SQL.OUT
\TIMING ON
SELECT
	O_ORDERPRIORITY,
	COUNT(*) AS ORDER_COUNT
FROM
	ORDERS
WHERE
	O_ORDERDATE >= DATE '1993-04-01'
	AND O_ORDERDATE < DATE '1993-04-01' + INTERVAL '3' MONTH
	AND EXISTS (
		SELECT
			*
		FROM
			LINEITEM
		WHERE
			L_ORDERKEY = O_ORDERKEY
			AND L_COMMITDATE < L_RECEIPTDATE
	)
GROUP BY
	O_ORDERPRIORITY
ORDER BY
	O_ORDERPRIORITY
;
COMMIT;
