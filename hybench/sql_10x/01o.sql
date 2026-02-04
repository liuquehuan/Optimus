select sourceid,targetid,
case when sourceid= 160365 then 'outbound' when targetid= 160365 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 160365 or targetid = 160365
group by sourceid,targetid
order by total_amount desc;