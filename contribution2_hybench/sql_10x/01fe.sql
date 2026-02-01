select sourceid,targetid,
case when sourceid= 459759 then 'outbound' when targetid= 459759 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 459759 or targetid = 459759
group by sourceid,targetid
order by total_amount desc;