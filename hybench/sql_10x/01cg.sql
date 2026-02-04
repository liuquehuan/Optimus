select sourceid,targetid,
case when sourceid= 2909806 then 'outbound' when targetid= 2909806 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2909806 or targetid = 2909806
group by sourceid,targetid
order by total_amount desc;