select sourceid,targetid,
case when sourceid= 152324 then 'outbound' when targetid= 152324 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 152324 or targetid = 152324
group by sourceid,targetid
order by total_amount desc;