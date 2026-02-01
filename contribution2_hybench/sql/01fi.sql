select sourceid,targetid,
case when sourceid= 13774 then 'outbound' when targetid= 13774 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 13774 or targetid = 13774
group by sourceid,targetid
order by total_amount desc;