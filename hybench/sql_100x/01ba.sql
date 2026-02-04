select sourceid,targetid,
case when sourceid= 29342405 then 'outbound' when targetid= 29342405 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 29342405 or targetid = 29342405
group by sourceid,targetid
order by total_amount desc;