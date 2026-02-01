select sourceid,targetid,
case when sourceid= 25256759 then 'outbound' when targetid= 25256759 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 25256759 or targetid = 25256759
group by sourceid,targetid
order by total_amount desc;