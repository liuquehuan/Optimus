select sourceid,targetid,
case when sourceid= 238900 then 'outbound' when targetid= 238900 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 238900 or targetid = 238900
group by sourceid,targetid
order by total_amount desc;