select sourceid,targetid,
case when sourceid= 991403 then 'outbound' when targetid= 991403 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 991403 or targetid = 991403
group by sourceid,targetid
order by total_amount desc;