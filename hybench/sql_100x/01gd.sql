select sourceid,targetid,
case when sourceid= 29551872 then 'outbound' when targetid= 29551872 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 29551872 or targetid = 29551872
group by sourceid,targetid
order by total_amount desc;