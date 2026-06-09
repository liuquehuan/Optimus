select sourceid,targetid,
case when sourceid= 1980692 then 'outbound' when targetid= 1980692 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1980692 or targetid = 1980692
group by sourceid,targetid
order by total_amount desc;