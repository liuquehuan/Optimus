select sourceid,targetid,
case when sourceid= 1327097 then 'outbound' when targetid= 1327097 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1327097 or targetid = 1327097
group by sourceid,targetid
order by total_amount desc;