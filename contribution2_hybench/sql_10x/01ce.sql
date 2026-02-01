select sourceid,targetid,
case when sourceid= 2052969 then 'outbound' when targetid= 2052969 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2052969 or targetid = 2052969
group by sourceid,targetid
order by total_amount desc;