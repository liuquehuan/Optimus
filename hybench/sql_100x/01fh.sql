select sourceid,targetid,
case when sourceid= 8431359 then 'outbound' when targetid= 8431359 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 8431359 or targetid = 8431359
group by sourceid,targetid
order by total_amount desc;