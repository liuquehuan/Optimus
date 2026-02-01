select sourceid,targetid,
case when sourceid= 211520 then 'outbound' when targetid= 211520 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 211520 or targetid = 211520
group by sourceid,targetid
order by total_amount desc;