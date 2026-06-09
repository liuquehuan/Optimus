select sourceid,targetid,
case when sourceid= 18457024 then 'outbound' when targetid= 18457024 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 18457024 or targetid = 18457024
group by sourceid,targetid
order by total_amount desc;