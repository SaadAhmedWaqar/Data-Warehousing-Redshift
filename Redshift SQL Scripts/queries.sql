-- Top Selling Products:
-- Find the top 10 best-selling products based on the total quantity sold. Include the product name, category, and the total quantity sold.

select dp.productname, dp.category, sum (fod.quantity) as Qty_Sold
from fct_order_details fod
join dim_products dp on fod.product_skey = dp.product_skey
group by (dp.productname, dp.category)
order by qty_sold desc
limit 10;


-- Monthly Sales Trend:
-- Determine the monthly sales trend by calculating the total sales revenue for each month in a given year.

select dd.month_num, dd.month_name, sum  (fod.grand_total) as Monthly_Sale
from fct_order_details fod
join dim_date dd on fod.date_key = dd.date_key
where dd.year = 2023
group by dd.month_num, dd.month_name
order by  dd.month_num


-- Sales Performance Comparison:
-- Compare the sales performance of different stores by calculating the total sales revenue for each store. Rank the stores based on their sales revenue 

select ds.storename, sum (fod.grand_total) as Sales_Revenue
from fct_order_details fod
join dim_stores ds on fod.store_skey = ds.store_skey
group by ds.storename;


-- Seasonal Sales Analysis:
-- Analyze the seasonal sales patterns by calculating the total sales revenue for each quarter of a given year. Identify the quarter with the highest sales revenue.

select dd.quarter, sum (fod.grand_total) as Sales_Revenue
from fct_order_details fod
join dim_date dd on fod.date_key = dd.date_key
group by dd.quarter


-- Average Order Value:
-- Calculate the average order value by dividing the total sales revenue by the total number of orders.

select avg (fod.grand_total) as Avg_Sales_Revenue
from fct_order_details fod

select sum (fod.total) / count (fod.orderid) as  Avg_Sales_Revenue
from fct_order_details fod

-- Store Location Analysis:
-- Analyze the sales performance based on store location by calculating the total sales revenue for each city or state

select ds.storename, ds.city, ds.state, sum (fod.total) as Sales_Revenue
from fct_order_details fod
join dim_stores ds on fod.store_skey = ds.store_skey
group by ds.storename, ds.city, ds.state
order by Sales_Revenue desc;


select * from fct_order_details

select * from dim_customers;

select * from dim_products;

select * from orders;
select * from stg_orders;
select * from fct_order_details;

select * from stg_fct_order_details;

select * from fct_order_details;


select * from customers;;
select * from stg_customers;
select * from dim_customers;









