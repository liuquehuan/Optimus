select sourceid,targetid,
case when sourceid= 9833 then 'outbound' when targetid= 9833 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 9833 or targetid = 9833
group by sourceid,targetid
order by total_amount desc;