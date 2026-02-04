select sourceid,targetid,
case when sourceid= 17975043 then 'outbound' when targetid= 17975043 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 17975043 or targetid = 17975043
group by sourceid,targetid
order by total_amount desc;