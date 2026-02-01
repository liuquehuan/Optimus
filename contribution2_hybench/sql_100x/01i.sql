select sourceid,targetid,
case when sourceid= 15191634 then 'outbound' when targetid= 15191634 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 15191634 or targetid = 15191634
group by sourceid,targetid
order by total_amount desc;