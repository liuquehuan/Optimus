select sourceid,targetid,
case when sourceid= 2250856 then 'outbound' when targetid= 2250856 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2250856 or targetid = 2250856
group by sourceid,targetid
order by total_amount desc;