select sourceid,targetid,
case when sourceid= 1798239 then 'outbound' when targetid= 1798239 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1798239 or targetid = 1798239
group by sourceid,targetid
order by total_amount desc;