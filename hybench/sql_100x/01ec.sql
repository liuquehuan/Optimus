select sourceid,targetid,
case when sourceid= 28860969 then 'outbound' when targetid= 28860969 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 28860969 or targetid = 28860969
group by sourceid,targetid
order by total_amount desc;