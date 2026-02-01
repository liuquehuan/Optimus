select sourceid,targetid,
case when sourceid= 228722 then 'outbound' when targetid= 228722 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 228722 or targetid = 228722
group by sourceid,targetid
order by total_amount desc;