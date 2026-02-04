select sourceid,targetid,
case when sourceid= 982072 then 'outbound' when targetid= 982072 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 982072 or targetid = 982072
group by sourceid,targetid
order by total_amount desc;