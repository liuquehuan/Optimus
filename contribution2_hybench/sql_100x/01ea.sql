select sourceid,targetid,
case when sourceid= 16563882 then 'outbound' when targetid= 16563882 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 16563882 or targetid = 16563882
group by sourceid,targetid
order by total_amount desc;