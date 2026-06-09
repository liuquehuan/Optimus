select sourceid,targetid,
case when sourceid= 2503978 then 'outbound' when targetid= 2503978 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2503978 or targetid = 2503978
group by sourceid,targetid
order by total_amount desc;