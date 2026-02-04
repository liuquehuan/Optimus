select sourceid,targetid,
case when sourceid= 23154209 then 'outbound' when targetid= 23154209 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 23154209 or targetid = 23154209
group by sourceid,targetid
order by total_amount desc;