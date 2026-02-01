select sourceid,targetid,
case when sourceid= 94806 then 'outbound' when targetid= 94806 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 94806 or targetid = 94806
group by sourceid,targetid
order by total_amount desc;