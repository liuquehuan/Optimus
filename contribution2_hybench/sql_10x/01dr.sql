select sourceid,targetid,
case when sourceid= 920322 then 'outbound' when targetid= 920322 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 920322 or targetid = 920322
group by sourceid,targetid
order by total_amount desc;