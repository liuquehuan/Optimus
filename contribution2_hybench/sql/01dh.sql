select sourceid,targetid,
case when sourceid= 273777 then 'outbound' when targetid= 273777 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 273777 or targetid = 273777
group by sourceid,targetid
order by total_amount desc;