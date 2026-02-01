select sourceid,targetid,
case when sourceid= 21365677 then 'outbound' when targetid= 21365677 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 21365677 or targetid = 21365677
group by sourceid,targetid
order by total_amount desc;