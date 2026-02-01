select sourceid,targetid,
case when sourceid= 181905 then 'outbound' when targetid= 181905 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 181905 or targetid = 181905
group by sourceid,targetid
order by total_amount desc;