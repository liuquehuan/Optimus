select sourceid,targetid,
case when sourceid= 259103 then 'outbound' when targetid= 259103 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 259103 or targetid = 259103
group by sourceid,targetid
order by total_amount desc;