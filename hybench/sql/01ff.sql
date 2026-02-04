select sourceid,targetid,
case when sourceid= 162633 then 'outbound' when targetid= 162633 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 162633 or targetid = 162633
group by sourceid,targetid
order by total_amount desc;