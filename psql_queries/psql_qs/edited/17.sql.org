-- using 1544205479 as a seed to the RNG


BEGIN;
\o 17.sql.out
\timing on
select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	lineitem,
	part
where
	p_partkey = l_partkey
	and p_brand = 'Brand#24'
	and p_container = 'MED CASE'
	and l_quantity < (
		select
			0.2 * avg(l_quantity)
		from
			lineitem
		where
			l_partkey = p_partkey
	)
;
COMMIT;
