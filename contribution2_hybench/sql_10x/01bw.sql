select sourceid,targetid,
case when sourceid= 276137 then 'outbound' when targetid= 276137 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 276137 or targetid = 276137
group by sourceid,targetid
order by total_amount desc;