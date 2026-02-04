select sourceid,targetid,
case when sourceid= 6184239 then 'outbound' when targetid= 6184239 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 6184239 or targetid = 6184239
group by sourceid,targetid
order by total_amount desc;