select sourceid,targetid,
case when sourceid= 17886902 then 'outbound' when targetid= 17886902 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 17886902 or targetid = 17886902
group by sourceid,targetid
order by total_amount desc;