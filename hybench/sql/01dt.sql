select sourceid,targetid,
case when sourceid= 125942 then 'outbound' when targetid= 125942 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 125942 or targetid = 125942
group by sourceid,targetid
order by total_amount desc;