select sourceid,targetid,
case when sourceid= 9121871 then 'outbound' when targetid= 9121871 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 9121871 or targetid = 9121871
group by sourceid,targetid
order by total_amount desc;