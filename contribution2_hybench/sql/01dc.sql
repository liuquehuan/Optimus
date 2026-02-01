select sourceid,targetid,
case when sourceid= 61015 then 'outbound' when targetid= 61015 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 61015 or targetid = 61015
group by sourceid,targetid
order by total_amount desc;