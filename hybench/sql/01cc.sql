select sourceid,targetid,
case when sourceid= 137031 then 'outbound' when targetid= 137031 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 137031 or targetid = 137031
group by sourceid,targetid
order by total_amount desc;