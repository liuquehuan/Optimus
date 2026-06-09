select sourceid,targetid,
case when sourceid= 69937 then 'outbound' when targetid= 69937 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 69937 or targetid = 69937
group by sourceid,targetid
order by total_amount desc;