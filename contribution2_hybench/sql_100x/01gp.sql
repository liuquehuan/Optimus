select sourceid,targetid,
case when sourceid= 3737573 then 'outbound' when targetid= 3737573 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 3737573 or targetid = 3737573
group by sourceid,targetid
order by total_amount desc;