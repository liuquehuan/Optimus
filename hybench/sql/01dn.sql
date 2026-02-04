select sourceid,targetid,
case when sourceid= 204789 then 'outbound' when targetid= 204789 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 204789 or targetid = 204789
group by sourceid,targetid
order by total_amount desc;