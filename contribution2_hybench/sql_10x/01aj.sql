select sourceid,targetid,
case when sourceid= 1510901 then 'outbound' when targetid= 1510901 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1510901 or targetid = 1510901
group by sourceid,targetid
order by total_amount desc;