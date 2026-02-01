select sourceid,targetid,
case when sourceid= 508103 then 'outbound' when targetid= 508103 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 508103 or targetid = 508103
group by sourceid,targetid
order by total_amount desc;