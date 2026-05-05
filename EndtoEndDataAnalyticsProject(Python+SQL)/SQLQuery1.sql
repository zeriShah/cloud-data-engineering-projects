SELECT order_id FROM df_orders
WHERE state = 'california';

SELECT * FROM df_orders;

SELECT state, category, SUM(cost_price) AS Total_cost_per_state FROM df_orders
GROUP BY state, category 
ORDER BY state

SELECT state, region, SUM(cost_price) AS Total_cost_per_state FROM df_orders
GROUP BY state, region
ORDER BY state

SELECT state, category, SUM(cost_price) AS Total_cost_per_state FROM df_orders
GROUP BY 
	GROUPING SETS ((state, category), (state), (category), ())
ORDER BY state

SELECT * FROM df_orders





with cte as (
    select region, product_id, sum(cost_price) as cost
    from df_orders
    group by region, product_id
)
select * from (
    select *, row_number() over(partition by region order by cost desc) as rn
    from cte
) A
where rn <= 10










with cte as (
    select 
        year(order_date) as order_year,
        month(order_date) as order_month,
        sum(sale_price) as sales
    from df_orders
    group by year(order_date), month(order_date)
)
select 
    order_year,
    sum(case when order_month = 2 then sales else 0 end) as feb_sales,
    sum(case when order_month = 3 then sales else 0 end) as march_sales
from cte
group by order_year
order by order_year;

-- Total Sale per product

select product_id, sum(sale_price) as total_sales
from df_orders
group by product_id
order by total_sales desc;

-- 




SELECT t.region, t.product_id, t.total_sales
FROM (
    SELECT region, product_id, SUM(sale_price) AS total_sales,
           ROW_NUMBER() OVER(PARTITION BY region ORDER BY SUM(sale_price) DESC) AS rn
    FROM df_orders
    GROUP BY region, product_id
) t
WHERE t.rn <= 5
ORDER BY t.region, t.total_sales DESC;




SELECT TOP 1 sub_category,
       SUM(CASE WHEN YEAR(order_date) = 2022 THEN sale_price ELSE 0 END) AS sales_2022,
       SUM(CASE WHEN YEAR(order_date) = 2023 THEN sale_price ELSE 0 END) AS sales_2023,
       SUM(CASE WHEN YEAR(order_date) = 2023 THEN sale_price ELSE 0 END) -
       SUM(CASE WHEN YEAR(order_date) = 2022 THEN sale_price ELSE 0 END) AS growth
FROM df_orders
GROUP BY sub_category
ORDER BY growth DESC;







