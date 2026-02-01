select sourceid,targetid,
case when sourceid= 2673854 then 'outbound' when targetid= 2673854 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2673854 or targetid = 2673854
group by sourceid,targetid
order by total_amount desc;