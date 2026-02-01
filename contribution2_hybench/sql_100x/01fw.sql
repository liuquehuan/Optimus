select sourceid,targetid,
case when sourceid= 15344291 then 'outbound' when targetid= 15344291 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 15344291 or targetid = 15344291
group by sourceid,targetid
order by total_amount desc;