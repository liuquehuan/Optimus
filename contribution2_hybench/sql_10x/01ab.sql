select sourceid,targetid,
case when sourceid= 1598871 then 'outbound' when targetid= 1598871 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1598871 or targetid = 1598871
group by sourceid,targetid
order by total_amount desc;