select sourceid,targetid,
case when sourceid= 6543014 then 'outbound' when targetid= 6543014 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 6543014 or targetid = 6543014
group by sourceid,targetid
order by total_amount desc;