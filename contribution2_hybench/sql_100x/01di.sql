select sourceid,targetid,
case when sourceid= 28151755 then 'outbound' when targetid= 28151755 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 28151755 or targetid = 28151755
group by sourceid,targetid
order by total_amount desc;