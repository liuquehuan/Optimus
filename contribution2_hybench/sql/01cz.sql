select sourceid,targetid,
case when sourceid= 244709 then 'outbound' when targetid= 244709 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 244709 or targetid = 244709
group by sourceid,targetid
order by total_amount desc;