select sourceid,targetid,
case when sourceid= 76922 then 'outbound' when targetid= 76922 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 76922 or targetid = 76922
group by sourceid,targetid
order by total_amount desc;