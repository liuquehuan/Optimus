select sourceid,targetid,
case when sourceid= 21650507 then 'outbound' when targetid= 21650507 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 21650507 or targetid = 21650507
group by sourceid,targetid
order by total_amount desc;