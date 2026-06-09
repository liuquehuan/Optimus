select sourceid,targetid,
case when sourceid= 155788 then 'outbound' when targetid= 155788 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 155788 or targetid = 155788
group by sourceid,targetid
order by total_amount desc;