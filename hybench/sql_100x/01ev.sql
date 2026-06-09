select sourceid,targetid,
case when sourceid= 6273935 then 'outbound' when targetid= 6273935 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 6273935 or targetid = 6273935
group by sourceid,targetid
order by total_amount desc;