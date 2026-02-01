select sourceid,targetid,
case when sourceid= 154715 then 'outbound' when targetid= 154715 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 154715 or targetid = 154715
group by sourceid,targetid
order by total_amount desc;