select sourceid,targetid,
case when sourceid= 47244 then 'outbound' when targetid= 47244 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 47244 or targetid = 47244
group by sourceid,targetid
order by total_amount desc;