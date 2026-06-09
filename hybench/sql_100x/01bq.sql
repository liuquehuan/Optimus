select sourceid,targetid,
case when sourceid= 8769978 then 'outbound' when targetid= 8769978 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 8769978 or targetid = 8769978
group by sourceid,targetid
order by total_amount desc;