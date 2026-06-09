select sourceid,targetid,
case when sourceid= 8608152 then 'outbound' when targetid= 8608152 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 8608152 or targetid = 8608152
group by sourceid,targetid
order by total_amount desc;