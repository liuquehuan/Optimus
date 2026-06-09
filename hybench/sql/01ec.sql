select sourceid,targetid,
case when sourceid= 106829 then 'outbound' when targetid= 106829 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 106829 or targetid = 106829
group by sourceid,targetid
order by total_amount desc;