select sourceid,targetid,
case when sourceid= 19142217 then 'outbound' when targetid= 19142217 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 19142217 or targetid = 19142217
group by sourceid,targetid
order by total_amount desc;