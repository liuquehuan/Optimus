select sourceid,targetid,
case when sourceid= 943664 then 'outbound' when targetid= 943664 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 943664 or targetid = 943664
group by sourceid,targetid
order by total_amount desc;