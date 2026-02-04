select sourceid,targetid,
case when sourceid= 502815 then 'outbound' when targetid= 502815 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 502815 or targetid = 502815
group by sourceid,targetid
order by total_amount desc;