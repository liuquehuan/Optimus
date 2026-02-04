select sourceid,targetid,
case when sourceid= 8261967 then 'outbound' when targetid= 8261967 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 8261967 or targetid = 8261967
group by sourceid,targetid
order by total_amount desc;