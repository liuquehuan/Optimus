select sourceid,targetid,
case when sourceid= 112767 then 'outbound' when targetid= 112767 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 112767 or targetid = 112767
group by sourceid,targetid
order by total_amount desc;