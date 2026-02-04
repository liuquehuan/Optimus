select sourceid,targetid,
case when sourceid= 504244 then 'outbound' when targetid= 504244 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 504244 or targetid = 504244
group by sourceid,targetid
order by total_amount desc;