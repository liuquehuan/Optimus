select sourceid,targetid,
case when sourceid= 89770 then 'outbound' when targetid= 89770 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 89770 or targetid = 89770
group by sourceid,targetid
order by total_amount desc;