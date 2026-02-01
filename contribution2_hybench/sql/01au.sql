select sourceid,targetid,
case when sourceid= 82922 then 'outbound' when targetid= 82922 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 82922 or targetid = 82922
group by sourceid,targetid
order by total_amount desc;