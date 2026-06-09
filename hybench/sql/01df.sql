select sourceid,targetid,
case when sourceid= 118657 then 'outbound' when targetid= 118657 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 118657 or targetid = 118657
group by sourceid,targetid
order by total_amount desc;