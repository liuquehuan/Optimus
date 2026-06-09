select sourceid,targetid,
case when sourceid= 238667 then 'outbound' when targetid= 238667 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 238667 or targetid = 238667
group by sourceid,targetid
order by total_amount desc;