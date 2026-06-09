select sourceid,targetid,
case when sourceid= 7324592 then 'outbound' when targetid= 7324592 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 7324592 or targetid = 7324592
group by sourceid,targetid
order by total_amount desc;