select sourceid,targetid,
case when sourceid= 577703 then 'outbound' when targetid= 577703 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 577703 or targetid = 577703
group by sourceid,targetid
order by total_amount desc;