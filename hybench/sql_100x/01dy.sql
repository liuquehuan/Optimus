select sourceid,targetid,
case when sourceid= 28516892 then 'outbound' when targetid= 28516892 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 28516892 or targetid = 28516892
group by sourceid,targetid
order by total_amount desc;