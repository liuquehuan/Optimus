select sourceid,targetid,
case when sourceid= 74875 then 'outbound' when targetid= 74875 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 74875 or targetid = 74875
group by sourceid,targetid
order by total_amount desc;