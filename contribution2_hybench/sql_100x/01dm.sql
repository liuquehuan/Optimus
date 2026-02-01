select sourceid,targetid,
case when sourceid= 21961333 then 'outbound' when targetid= 21961333 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 21961333 or targetid = 21961333
group by sourceid,targetid
order by total_amount desc;