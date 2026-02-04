select sourceid,targetid,
case when sourceid= 657755 then 'outbound' when targetid= 657755 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 657755 or targetid = 657755
group by sourceid,targetid
order by total_amount desc;