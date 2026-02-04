select sourceid,targetid,
case when sourceid= 16209679 then 'outbound' when targetid= 16209679 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 16209679 or targetid = 16209679
group by sourceid,targetid
order by total_amount desc;