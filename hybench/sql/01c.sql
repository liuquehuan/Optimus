select sourceid,targetid,
case when sourceid= 218740 then 'outbound' when targetid= 218740 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 218740 or targetid = 218740
group by sourceid,targetid
order by total_amount desc;