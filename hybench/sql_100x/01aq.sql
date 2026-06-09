select sourceid,targetid,
case when sourceid= 8596695 then 'outbound' when targetid= 8596695 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 8596695 or targetid = 8596695
group by sourceid,targetid
order by total_amount desc;