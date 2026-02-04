select sourceid,targetid,
case when sourceid= 168898 then 'outbound' when targetid= 168898 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 168898 or targetid = 168898
group by sourceid,targetid
order by total_amount desc;