select sourceid,targetid,
case when sourceid= 29827 then 'outbound' when targetid= 29827 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 29827 or targetid = 29827
group by sourceid,targetid
order by total_amount desc;