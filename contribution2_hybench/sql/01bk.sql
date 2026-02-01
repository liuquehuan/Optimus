select sourceid,targetid,
case when sourceid= 189864 then 'outbound' when targetid= 189864 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 189864 or targetid = 189864
group by sourceid,targetid
order by total_amount desc;