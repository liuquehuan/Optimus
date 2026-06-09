select sourceid,targetid,
case when sourceid= 17887490 then 'outbound' when targetid= 17887490 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 17887490 or targetid = 17887490
group by sourceid,targetid
order by total_amount desc;