select sourceid,targetid,
case when sourceid= 226591 then 'outbound' when targetid= 226591 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 226591 or targetid = 226591
group by sourceid,targetid
order by total_amount desc;