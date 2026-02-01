select sourceid,targetid,
case when sourceid= 9603080 then 'outbound' when targetid= 9603080 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 9603080 or targetid = 9603080
group by sourceid,targetid
order by total_amount desc;