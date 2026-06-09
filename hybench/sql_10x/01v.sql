select sourceid,targetid,
case when sourceid= 973079 then 'outbound' when targetid= 973079 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 973079 or targetid = 973079
group by sourceid,targetid
order by total_amount desc;