select sourceid,targetid,
case when sourceid= 13547665 then 'outbound' when targetid= 13547665 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 13547665 or targetid = 13547665
group by sourceid,targetid
order by total_amount desc;