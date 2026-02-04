select sourceid,targetid,
case when sourceid= 45682 then 'outbound' when targetid= 45682 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 45682 or targetid = 45682
group by sourceid,targetid
order by total_amount desc;