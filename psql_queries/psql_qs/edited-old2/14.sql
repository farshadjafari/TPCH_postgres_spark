-- using 1544205479 as a seed to the RNG


\o 14.sql.out
explain
select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date '1996-10-01'
	and l_shipdate < date '1996-10-01' + interval '1' month
;