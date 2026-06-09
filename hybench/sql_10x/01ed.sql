select sourceid,targetid,
case when sourceid= 2544529 then 'outbound' when targetid= 2544529 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2544529 or targetid = 2544529
group by sourceid,targetid
order by total_amount desc;