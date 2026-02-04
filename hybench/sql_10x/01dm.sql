select sourceid,targetid,
case when sourceid= 1430520 then 'outbound' when targetid= 1430520 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1430520 or targetid = 1430520
group by sourceid,targetid
order by total_amount desc;