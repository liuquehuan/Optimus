select sourceid,targetid,
case when sourceid= 18817334 then 'outbound' when targetid= 18817334 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 18817334 or targetid = 18817334
group by sourceid,targetid
order by total_amount desc;