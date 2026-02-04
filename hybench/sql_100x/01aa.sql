select sourceid,targetid,
case when sourceid= 14187790 then 'outbound' when targetid= 14187790 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 14187790 or targetid = 14187790
group by sourceid,targetid
order by total_amount desc;