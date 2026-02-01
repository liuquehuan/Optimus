select sourceid,targetid,
case when sourceid= 182979 then 'outbound' when targetid= 182979 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 182979 or targetid = 182979
group by sourceid,targetid
order by total_amount desc;