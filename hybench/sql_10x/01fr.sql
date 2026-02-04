select sourceid,targetid,
case when sourceid= 1955136 then 'outbound' when targetid= 1955136 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1955136 or targetid = 1955136
group by sourceid,targetid
order by total_amount desc;