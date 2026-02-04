select sourceid,targetid,
case when sourceid= 1906728 then 'outbound' when targetid= 1906728 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1906728 or targetid = 1906728
group by sourceid,targetid
order by total_amount desc;