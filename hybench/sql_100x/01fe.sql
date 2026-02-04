select sourceid,targetid,
case when sourceid= 7184566 then 'outbound' when targetid= 7184566 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 7184566 or targetid = 7184566
group by sourceid,targetid
order by total_amount desc;