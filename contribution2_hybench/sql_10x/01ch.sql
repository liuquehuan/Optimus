select sourceid,targetid,
case when sourceid= 935974 then 'outbound' when targetid= 935974 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 935974 or targetid = 935974
group by sourceid,targetid
order by total_amount desc;