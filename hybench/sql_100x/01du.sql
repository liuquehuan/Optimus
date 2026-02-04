select sourceid,targetid,
case when sourceid= 12052412 then 'outbound' when targetid= 12052412 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 12052412 or targetid = 12052412
group by sourceid,targetid
order by total_amount desc;