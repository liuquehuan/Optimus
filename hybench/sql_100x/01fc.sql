select sourceid,targetid,
case when sourceid= 565074 then 'outbound' when targetid= 565074 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 565074 or targetid = 565074
group by sourceid,targetid
order by total_amount desc;