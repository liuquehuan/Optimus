select sourceid,targetid,
case when sourceid= 18813813 then 'outbound' when targetid= 18813813 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 18813813 or targetid = 18813813
group by sourceid,targetid
order by total_amount desc;