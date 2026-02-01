select sourceid,targetid,
case when sourceid= 24207624 then 'outbound' when targetid= 24207624 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 24207624 or targetid = 24207624
group by sourceid,targetid
order by total_amount desc;