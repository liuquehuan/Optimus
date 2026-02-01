select sourceid,targetid,
case when sourceid= 53796 then 'outbound' when targetid= 53796 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 53796 or targetid = 53796
group by sourceid,targetid
order by total_amount desc;