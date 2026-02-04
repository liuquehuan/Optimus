select sourceid,targetid,
case when sourceid= 336306 then 'outbound' when targetid= 336306 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 336306 or targetid = 336306
group by sourceid,targetid
order by total_amount desc;