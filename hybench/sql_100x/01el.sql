select sourceid,targetid,
case when sourceid= 21134692 then 'outbound' when targetid= 21134692 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 21134692 or targetid = 21134692
group by sourceid,targetid
order by total_amount desc;