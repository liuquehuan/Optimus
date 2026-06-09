select sourceid,targetid,
case when sourceid= 2096076 then 'outbound' when targetid= 2096076 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2096076 or targetid = 2096076
group by sourceid,targetid
order by total_amount desc;