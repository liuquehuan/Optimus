select sourceid,targetid,
case when sourceid= 1781779 then 'outbound' when targetid= 1781779 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1781779 or targetid = 1781779
group by sourceid,targetid
order by total_amount desc;