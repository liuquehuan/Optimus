select sourceid,targetid,
case when sourceid= 1389047 then 'outbound' when targetid= 1389047 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1389047 or targetid = 1389047
group by sourceid,targetid
order by total_amount desc;