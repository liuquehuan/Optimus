select sourceid,targetid,
case when sourceid= 162229 then 'outbound' when targetid= 162229 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 162229 or targetid = 162229
group by sourceid,targetid
order by total_amount desc;