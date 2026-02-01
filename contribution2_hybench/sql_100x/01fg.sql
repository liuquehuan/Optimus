select sourceid,targetid,
case when sourceid= 26028603 then 'outbound' when targetid= 26028603 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 26028603 or targetid = 26028603
group by sourceid,targetid
order by total_amount desc;