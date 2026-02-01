select sourceid,targetid,
case when sourceid= 179103 then 'outbound' when targetid= 179103 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 179103 or targetid = 179103
group by sourceid,targetid
order by total_amount desc;