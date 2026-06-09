select sourceid,targetid,
case when sourceid= 28471283 then 'outbound' when targetid= 28471283 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 28471283 or targetid = 28471283
group by sourceid,targetid
order by total_amount desc;