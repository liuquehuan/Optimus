select sourceid,targetid,
case when sourceid= 17850307 then 'outbound' when targetid= 17850307 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 17850307 or targetid = 17850307
group by sourceid,targetid
order by total_amount desc;