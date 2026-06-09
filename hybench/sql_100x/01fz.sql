select sourceid,targetid,
case when sourceid= 19129607 then 'outbound' when targetid= 19129607 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 19129607 or targetid = 19129607
group by sourceid,targetid
order by total_amount desc;