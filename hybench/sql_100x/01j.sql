select sourceid,targetid,
case when sourceid= 7466134 then 'outbound' when targetid= 7466134 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 7466134 or targetid = 7466134
group by sourceid,targetid
order by total_amount desc;