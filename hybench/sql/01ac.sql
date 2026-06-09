select sourceid,targetid,
case when sourceid= 10231 then 'outbound' when targetid= 10231 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 10231 or targetid = 10231
group by sourceid,targetid
order by total_amount desc;