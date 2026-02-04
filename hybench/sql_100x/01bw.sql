select sourceid,targetid,
case when sourceid= 25639407 then 'outbound' when targetid= 25639407 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 25639407 or targetid = 25639407
group by sourceid,targetid
order by total_amount desc;