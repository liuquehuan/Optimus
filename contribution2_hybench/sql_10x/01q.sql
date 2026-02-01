select sourceid,targetid,
case when sourceid= 445970 then 'outbound' when targetid= 445970 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 445970 or targetid = 445970
group by sourceid,targetid
order by total_amount desc;