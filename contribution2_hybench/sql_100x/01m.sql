select sourceid,targetid,
case when sourceid= 8680329 then 'outbound' when targetid= 8680329 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 8680329 or targetid = 8680329
group by sourceid,targetid
order by total_amount desc;