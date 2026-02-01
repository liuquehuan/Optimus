select sourceid,targetid,
case when sourceid= 27229910 then 'outbound' when targetid= 27229910 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 27229910 or targetid = 27229910
group by sourceid,targetid
order by total_amount desc;