select sourceid,targetid,
case when sourceid= 586891 then 'outbound' when targetid= 586891 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 586891 or targetid = 586891
group by sourceid,targetid
order by total_amount desc;