select sourceid,targetid,
case when sourceid= 7527039 then 'outbound' when targetid= 7527039 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 7527039 or targetid = 7527039
group by sourceid,targetid
order by total_amount desc;