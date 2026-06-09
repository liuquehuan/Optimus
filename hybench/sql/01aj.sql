select sourceid,targetid,
case when sourceid= 213259 then 'outbound' when targetid= 213259 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 213259 or targetid = 213259
group by sourceid,targetid
order by total_amount desc;