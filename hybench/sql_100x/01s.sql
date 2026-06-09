select sourceid,targetid,
case when sourceid= 19297947 then 'outbound' when targetid= 19297947 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 19297947 or targetid = 19297947
group by sourceid,targetid
order by total_amount desc;