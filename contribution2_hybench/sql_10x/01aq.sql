select sourceid,targetid,
case when sourceid= 285687 then 'outbound' when targetid= 285687 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 285687 or targetid = 285687
group by sourceid,targetid
order by total_amount desc;