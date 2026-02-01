select sourceid,targetid,
case when sourceid= 13133492 then 'outbound' when targetid= 13133492 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 13133492 or targetid = 13133492
group by sourceid,targetid
order by total_amount desc;