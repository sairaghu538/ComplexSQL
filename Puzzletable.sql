drop table if exists puzzle;
create table puzzle(id int, rules varchar(10), value int)

INSERT into puzzle values (1, '1+3', 10);
INSERT into puzzle values (2, '2*5', 12);
INSERT into puzzle values (3, '3-1', 14);
INSERT into puzzle values (4, '4/1', 15);
INSERT into puzzle values (5, '5+4', 18);

select * from puzzle;

with cte as (
SELECT *,
       SUBSTRING(rules, 1, 1) AS f,
       SUBSTRING(rules, 2, 1) AS op,
       SUBSTRING(rules, -1, len(rules)) AS l
FROM puzzle)

select a.id, a.rules,
round(case when a.op ='+' then b.value+c.value
     when a.op ='-' then b.value-c.value
     when a.op ='*' then b.value*c.value
     when a.op ='/' then b.value/c.value
    end) as calculations
from cte as a
left join cte as b
on a.f = b.id
left join cte as c 
on a.l = c.id;

