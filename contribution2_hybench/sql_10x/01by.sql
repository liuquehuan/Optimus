select sourceid,targetid,
case when sourceid= 2961725 then 'outbound' when targetid= 2961725 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2961725 or targetid = 2961725
group by sourceid,targetid
order by total_amount desc;