select sourceid,targetid,
case when sourceid= 630064 then 'outbound' when targetid= 630064 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 630064 or targetid = 630064
group by sourceid,targetid
order by total_amount desc;