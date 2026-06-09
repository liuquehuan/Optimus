select sourceid,targetid,
case when sourceid= 17522711 then 'outbound' when targetid= 17522711 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 17522711 or targetid = 17522711
group by sourceid,targetid
order by total_amount desc;