select sourceid,targetid,
case when sourceid= 3409933 then 'outbound' when targetid= 3409933 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 3409933 or targetid = 3409933
group by sourceid,targetid
order by total_amount desc;