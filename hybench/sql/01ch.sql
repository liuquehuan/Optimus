select sourceid,targetid,
case when sourceid= 96030 then 'outbound' when targetid= 96030 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 96030 or targetid = 96030
group by sourceid,targetid
order by total_amount desc;