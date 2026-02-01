select sourceid,targetid,
case when sourceid= 15919475 then 'outbound' when targetid= 15919475 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 15919475 or targetid = 15919475
group by sourceid,targetid
order by total_amount desc;