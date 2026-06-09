select sourceid,targetid,
case when sourceid= 16361639 then 'outbound' when targetid= 16361639 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 16361639 or targetid = 16361639
group by sourceid,targetid
order by total_amount desc;