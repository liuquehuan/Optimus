select sourceid,targetid,
case when sourceid= 19276988 then 'outbound' when targetid= 19276988 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 19276988 or targetid = 19276988
group by sourceid,targetid
order by total_amount desc;