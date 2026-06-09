select sourceid,targetid,
case when sourceid= 79938 then 'outbound' when targetid= 79938 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 79938 or targetid = 79938
group by sourceid,targetid
order by total_amount desc;