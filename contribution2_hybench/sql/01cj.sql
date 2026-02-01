select sourceid,targetid,
case when sourceid= 89993 then 'outbound' when targetid= 89993 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 89993 or targetid = 89993
group by sourceid,targetid
order by total_amount desc;