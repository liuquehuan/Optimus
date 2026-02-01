select sourceid,targetid,
case when sourceid= 547693 then 'outbound' when targetid= 547693 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 547693 or targetid = 547693
group by sourceid,targetid
order by total_amount desc;