select sourceid,targetid,
case when sourceid= 6360694 then 'outbound' when targetid= 6360694 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 6360694 or targetid = 6360694
group by sourceid,targetid
order by total_amount desc;