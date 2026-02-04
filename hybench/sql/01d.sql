select sourceid,targetid,
case when sourceid= 13697 then 'outbound' when targetid= 13697 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 13697 or targetid = 13697
group by sourceid,targetid
order by total_amount desc;