with cte as(
select id, student,
lead(student) over(order by id) leading,
lag(student) over(order by id) lagging
from Seat
)
select id, 
case when id%2=0 then lagging else coalesce(leading,student) end as student
from cte