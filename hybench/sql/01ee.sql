select sourceid,targetid,
case when sourceid= 148233 then 'outbound' when targetid= 148233 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 148233 or targetid = 148233
group by sourceid,targetid
order by total_amount desc;