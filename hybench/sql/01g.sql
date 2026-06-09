select sourceid,targetid,
case when sourceid= 64886 then 'outbound' when targetid= 64886 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 64886 or targetid = 64886
group by sourceid,targetid
order by total_amount desc;