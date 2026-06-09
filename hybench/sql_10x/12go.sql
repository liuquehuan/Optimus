select co.companyid, count(case when lt.status='accept' then 1 else null end) * 100.0/count(la.id) as rate
from company co, loanapps la, loantrans lt
where co.category ='other_service' and co.companyid=la.applicantid and la.applicantid=lt.applicantid
group by co.companyid
order by rate DESC limit 10;