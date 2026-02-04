select sourceid,targetid,
case when sourceid= 6426937 then 'outbound' when targetid= 6426937 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 6426937 or targetid = 6426937
group by sourceid,targetid
order by total_amount desc;