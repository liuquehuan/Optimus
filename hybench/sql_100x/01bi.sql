select sourceid,targetid,
case when sourceid= 16136133 then 'outbound' when targetid= 16136133 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 16136133 or targetid = 16136133
group by sourceid,targetid
order by total_amount desc;