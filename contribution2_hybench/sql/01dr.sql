select sourceid,targetid,
case when sourceid= 151817 then 'outbound' when targetid= 151817 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 151817 or targetid = 151817
group by sourceid,targetid
order by total_amount desc;