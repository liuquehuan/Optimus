select sourceid,targetid,
case when sourceid= 19308757 then 'outbound' when targetid= 19308757 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 19308757 or targetid = 19308757
group by sourceid,targetid
order by total_amount desc;