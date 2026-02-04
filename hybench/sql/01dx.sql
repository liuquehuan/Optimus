select sourceid,targetid,
case when sourceid= 12726 then 'outbound' when targetid= 12726 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 12726 or targetid = 12726
group by sourceid,targetid
order by total_amount desc;