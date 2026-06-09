select sourceid,targetid,
case when sourceid= 1647151 then 'outbound' when targetid= 1647151 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1647151 or targetid = 1647151
group by sourceid,targetid
order by total_amount desc;