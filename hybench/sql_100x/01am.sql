select sourceid,targetid,
case when sourceid= 23654968 then 'outbound' when targetid= 23654968 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 23654968 or targetid = 23654968
group by sourceid,targetid
order by total_amount desc;