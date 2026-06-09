select sourceid,targetid,
case when sourceid= 26315719 then 'outbound' when targetid= 26315719 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 26315719 or targetid = 26315719
group by sourceid,targetid
order by total_amount desc;