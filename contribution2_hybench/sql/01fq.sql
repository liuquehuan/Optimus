select sourceid,targetid,
case when sourceid= 81891 then 'outbound' when targetid= 81891 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 81891 or targetid = 81891
group by sourceid,targetid
order by total_amount desc;