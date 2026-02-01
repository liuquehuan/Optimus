select sourceid,targetid,
case when sourceid= 2760220 then 'outbound' when targetid= 2760220 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2760220 or targetid = 2760220
group by sourceid,targetid
order by total_amount desc;