select sourceid,targetid,
case when sourceid= 2179315 then 'outbound' when targetid= 2179315 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2179315 or targetid = 2179315
group by sourceid,targetid
order by total_amount desc;