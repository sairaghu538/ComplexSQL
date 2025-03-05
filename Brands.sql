create or replace database brands;

use DATABASE brands;

CREATE or replace TABLE brands(
    category STRING,
    brand_name STRING
);

insert into brands(category, brand_name)
values
    ('chocolates', '5-star'),
    (NULL, 'dairy milk'),
    (NULL, 'perk'),
    (NULL, 'eclair'),
    ('Biscuits', 'britania'),
    (NULL, 'good day'),
    (NULL, 'boost')



select * from brands;


with cte as(
select *,
row_number() over (order by (select NULL)) as RN
from brands
),

cte1 as(
select *,
count(case when category is not null then 1 end) over (order by RN) as groups
from cte)

select 
FIRST_VALUE(category) over (partition by groups order by RN)as category, brand_name
from cte1;
