select sourceid,targetid,
case when sourceid= 16491305 then 'outbound' when targetid= 16491305 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 16491305 or targetid = 16491305
group by sourceid,targetid
order by total_amount desc;