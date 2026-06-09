select sourceid,targetid,
case when sourceid= 213302 then 'outbound' when targetid= 213302 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 213302 or targetid = 213302
group by sourceid,targetid
order by total_amount desc;