select sourceid,targetid,
case when sourceid= 1703464 then 'outbound' when targetid= 1703464 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1703464 or targetid = 1703464
group by sourceid,targetid
order by total_amount desc;