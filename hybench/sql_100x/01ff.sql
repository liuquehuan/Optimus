select sourceid,targetid,
case when sourceid= 25324667 then 'outbound' when targetid= 25324667 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 25324667 or targetid = 25324667
group by sourceid,targetid
order by total_amount desc;