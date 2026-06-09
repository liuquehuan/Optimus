select sourceid,targetid,
case when sourceid= 127153 then 'outbound' when targetid= 127153 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 127153 or targetid = 127153
group by sourceid,targetid
order by total_amount desc;