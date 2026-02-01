select sourceid,targetid,
case when sourceid= 8740075 then 'outbound' when targetid= 8740075 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 8740075 or targetid = 8740075
group by sourceid,targetid
order by total_amount desc;