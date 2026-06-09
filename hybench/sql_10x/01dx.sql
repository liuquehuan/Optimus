select sourceid,targetid,
case when sourceid= 1508711 then 'outbound' when targetid= 1508711 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1508711 or targetid = 1508711
group by sourceid,targetid
order by total_amount desc;