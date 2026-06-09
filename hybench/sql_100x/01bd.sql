select sourceid,targetid,
case when sourceid= 6637186 then 'outbound' when targetid= 6637186 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 6637186 or targetid = 6637186
group by sourceid,targetid
order by total_amount desc;