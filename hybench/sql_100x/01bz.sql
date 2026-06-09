select sourceid,targetid,
case when sourceid= 17313769 then 'outbound' when targetid= 17313769 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 17313769 or targetid = 17313769
group by sourceid,targetid
order by total_amount desc;