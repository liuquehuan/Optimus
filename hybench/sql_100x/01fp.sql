select sourceid,targetid,
case when sourceid= 3654329 then 'outbound' when targetid= 3654329 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 3654329 or targetid = 3654329
group by sourceid,targetid
order by total_amount desc;