select sourceid,targetid,
case when sourceid= 1719609 then 'outbound' when targetid= 1719609 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1719609 or targetid = 1719609
group by sourceid,targetid
order by total_amount desc;