select sourceid,targetid,
case when sourceid= 626986 then 'outbound' when targetid= 626986 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 626986 or targetid = 626986
group by sourceid,targetid
order by total_amount desc;