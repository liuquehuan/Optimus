select sourceid,targetid,
case when sourceid= 1392763 then 'outbound' when targetid= 1392763 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1392763 or targetid = 1392763
group by sourceid,targetid
order by total_amount desc;