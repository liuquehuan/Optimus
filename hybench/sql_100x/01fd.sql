select sourceid,targetid,
case when sourceid= 1082223 then 'outbound' when targetid= 1082223 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1082223 or targetid = 1082223
group by sourceid,targetid
order by total_amount desc;