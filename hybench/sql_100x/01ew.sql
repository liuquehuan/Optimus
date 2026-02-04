select sourceid,targetid,
case when sourceid= 10305015 then 'outbound' when targetid= 10305015 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 10305015 or targetid = 10305015
group by sourceid,targetid
order by total_amount desc;