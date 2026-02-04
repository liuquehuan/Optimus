select sourceid,targetid,
case when sourceid= 13445670 then 'outbound' when targetid= 13445670 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 13445670 or targetid = 13445670
group by sourceid,targetid
order by total_amount desc;