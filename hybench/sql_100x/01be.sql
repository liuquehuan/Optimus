select sourceid,targetid,
case when sourceid= 3982897 then 'outbound' when targetid= 3982897 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 3982897 or targetid = 3982897
group by sourceid,targetid
order by total_amount desc;