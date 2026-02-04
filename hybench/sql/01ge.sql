select sourceid,targetid,
case when sourceid= 264807 then 'outbound' when targetid= 264807 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 264807 or targetid = 264807
group by sourceid,targetid
order by total_amount desc;