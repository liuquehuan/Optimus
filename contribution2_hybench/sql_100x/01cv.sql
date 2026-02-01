select sourceid,targetid,
case when sourceid= 20499238 then 'outbound' when targetid= 20499238 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 20499238 or targetid = 20499238
group by sourceid,targetid
order by total_amount desc;