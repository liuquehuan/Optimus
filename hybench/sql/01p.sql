select sourceid,targetid,
case when sourceid= 14596 then 'outbound' when targetid= 14596 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 14596 or targetid = 14596
group by sourceid,targetid
order by total_amount desc;