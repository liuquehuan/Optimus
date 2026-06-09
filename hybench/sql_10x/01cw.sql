select sourceid,targetid,
case when sourceid= 413794 then 'outbound' when targetid= 413794 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 413794 or targetid = 413794
group by sourceid,targetid
order by total_amount desc;