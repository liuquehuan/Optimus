select sourceid,targetid,
case when sourceid= 79844 then 'outbound' when targetid= 79844 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 79844 or targetid = 79844
group by sourceid,targetid
order by total_amount desc;