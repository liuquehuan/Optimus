select sourceid,targetid,
case when sourceid= 99869 then 'outbound' when targetid= 99869 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 99869 or targetid = 99869
group by sourceid,targetid
order by total_amount desc;