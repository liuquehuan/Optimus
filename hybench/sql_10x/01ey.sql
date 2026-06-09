select sourceid,targetid,
case when sourceid= 335324 then 'outbound' when targetid= 335324 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 335324 or targetid = 335324
group by sourceid,targetid
order by total_amount desc;