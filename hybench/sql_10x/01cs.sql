select sourceid,targetid,
case when sourceid= 427065 then 'outbound' when targetid= 427065 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 427065 or targetid = 427065
group by sourceid,targetid
order by total_amount desc;