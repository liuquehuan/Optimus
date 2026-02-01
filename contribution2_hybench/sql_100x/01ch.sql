select sourceid,targetid,
case when sourceid= 25621818 then 'outbound' when targetid= 25621818 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 25621818 or targetid = 25621818
group by sourceid,targetid
order by total_amount desc;