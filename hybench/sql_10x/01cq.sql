select sourceid,targetid,
case when sourceid= 855986 then 'outbound' when targetid= 855986 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 855986 or targetid = 855986
group by sourceid,targetid
order by total_amount desc;