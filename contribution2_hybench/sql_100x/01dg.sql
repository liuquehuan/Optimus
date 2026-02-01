select sourceid,targetid,
case when sourceid= 28199665 then 'outbound' when targetid= 28199665 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 28199665 or targetid = 28199665
group by sourceid,targetid
order by total_amount desc;