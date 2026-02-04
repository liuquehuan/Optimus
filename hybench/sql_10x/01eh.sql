select sourceid,targetid,
case when sourceid= 156251 then 'outbound' when targetid= 156251 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 156251 or targetid = 156251
group by sourceid,targetid
order by total_amount desc;