select sourceid,targetid,
case when sourceid= 3631714 then 'outbound' when targetid= 3631714 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 3631714 or targetid = 3631714
group by sourceid,targetid
order by total_amount desc;