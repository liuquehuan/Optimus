select sourceid,targetid,
case when sourceid= 824403 then 'outbound' when targetid= 824403 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 824403 or targetid = 824403
group by sourceid,targetid
order by total_amount desc;