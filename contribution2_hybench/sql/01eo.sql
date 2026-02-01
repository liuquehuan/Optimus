select sourceid,targetid,
case when sourceid= 137857 then 'outbound' when targetid= 137857 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 137857 or targetid = 137857
group by sourceid,targetid
order by total_amount desc;