select sourceid,targetid,
case when sourceid= 1961859 then 'outbound' when targetid= 1961859 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1961859 or targetid = 1961859
group by sourceid,targetid
order by total_amount desc;