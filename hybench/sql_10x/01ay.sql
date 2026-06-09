select sourceid,targetid,
case when sourceid= 815191 then 'outbound' when targetid= 815191 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 815191 or targetid = 815191
group by sourceid,targetid
order by total_amount desc;