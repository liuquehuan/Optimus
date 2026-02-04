select sourceid,targetid,
case when sourceid= 24169733 then 'outbound' when targetid= 24169733 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 24169733 or targetid = 24169733
group by sourceid,targetid
order by total_amount desc;