select sourceid,targetid,
case when sourceid= 1017128 then 'outbound' when targetid= 1017128 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1017128 or targetid = 1017128
group by sourceid,targetid
order by total_amount desc;