select sourceid,targetid,
case when sourceid= 89046 then 'outbound' when targetid= 89046 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 89046 or targetid = 89046
group by sourceid,targetid
order by total_amount desc;