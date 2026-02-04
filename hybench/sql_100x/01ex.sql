select sourceid,targetid,
case when sourceid= 21060753 then 'outbound' when targetid= 21060753 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 21060753 or targetid = 21060753
group by sourceid,targetid
order by total_amount desc;