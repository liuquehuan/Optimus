select sourceid,targetid,
case when sourceid= 679509 then 'outbound' when targetid= 679509 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 679509 or targetid = 679509
group by sourceid,targetid
order by total_amount desc;