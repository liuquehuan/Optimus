select sourceid,targetid,
case when sourceid= 109883 then 'outbound' when targetid= 109883 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 109883 or targetid = 109883
group by sourceid,targetid
order by total_amount desc;