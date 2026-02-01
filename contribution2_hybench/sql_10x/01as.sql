select sourceid,targetid,
case when sourceid= 196522 then 'outbound' when targetid= 196522 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 196522 or targetid = 196522
group by sourceid,targetid
order by total_amount desc;