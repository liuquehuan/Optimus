select sourceid,targetid,
case when sourceid= 903167 then 'outbound' when targetid= 903167 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 903167 or targetid = 903167
group by sourceid,targetid
order by total_amount desc;