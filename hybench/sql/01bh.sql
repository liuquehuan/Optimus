select sourceid,targetid,
case when sourceid= 184611 then 'outbound' when targetid= 184611 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 184611 or targetid = 184611
group by sourceid,targetid
order by total_amount desc;