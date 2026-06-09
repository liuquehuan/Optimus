select sourceid,targetid,
case when sourceid= 136106 then 'outbound' when targetid= 136106 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 136106 or targetid = 136106
group by sourceid,targetid
order by total_amount desc;