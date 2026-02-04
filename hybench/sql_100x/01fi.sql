select sourceid,targetid,
case when sourceid= 11636683 then 'outbound' when targetid= 11636683 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 11636683 or targetid = 11636683
group by sourceid,targetid
order by total_amount desc;