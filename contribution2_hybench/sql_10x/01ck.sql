select sourceid,targetid,
case when sourceid= 2184132 then 'outbound' when targetid= 2184132 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2184132 or targetid = 2184132
group by sourceid,targetid
order by total_amount desc;