select sourceid,targetid,
case when sourceid= 5070146 then 'outbound' when targetid= 5070146 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 5070146 or targetid = 5070146
group by sourceid,targetid
order by total_amount desc;