select sourceid,targetid,
case when sourceid= 177101 then 'outbound' when targetid= 177101 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 177101 or targetid = 177101
group by sourceid,targetid
order by total_amount desc;