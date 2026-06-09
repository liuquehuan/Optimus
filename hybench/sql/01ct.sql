select sourceid,targetid,
case when sourceid= 79709 then 'outbound' when targetid= 79709 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 79709 or targetid = 79709
group by sourceid,targetid
order by total_amount desc;