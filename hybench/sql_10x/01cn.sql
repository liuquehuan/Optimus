select sourceid,targetid,
case when sourceid= 505769 then 'outbound' when targetid= 505769 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 505769 or targetid = 505769
group by sourceid,targetid
order by total_amount desc;