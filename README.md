# Data-Warehousing-Redshift

## Problem: S3(Relational Data) to Star Schema ETL
### Part 1: Relational Schema Design:
1.	Customers (CustomerID, FirstName, LastName, Email, Address, City, State, ZipCode, LastModified)
2.	Products (ProductID, ProductName, Category, Description, Price, LastModified)
3.	Stores (StoreID, StoreName, Address, City, State, ZipCode, LastModified)
4.	Orders (OrderID, CustomerID, StoreID, OrderDate, TotalPrice, LastModified)
5.	OrderDetails (OrderID, ProductID, Quantity, Price, LastModified)

### Part 2: Data Population
Use ChatGPT or any other data generation method to populate the CSV files with random data for each table. Ensure that the data is realistic and consistent with the schema.

### Part 3: Data Import to S3 Bucket
•	Create an S3 bucket in your AWS account.
•	Upload these CSV files to the S3 bucket.

### Part 4: Data Copy to Redshift Cluster
•	Create a Redshift cluster in your AWS account.
•	Configure the necessary security groups, network settings, and access permissions.
•	Use the Redshift COPY command to load the data from the S3 bucket into the Redshift tables. Ensure that the tables are created in Redshift with the same schema as in the csv file.

### Part 5: Data Transformation to Star Schema (SCD Type 2) and Upserting using Staging Table
•	Create a star schema for retail sales.
•	Write SQL queries to transform the data from the relational schema into a star schema using SCD Type 2. Perform the necessary data transformations to populate these tables with the appropriate historical changes using SCD Type 2.
•	Update and Insert (upsert) the updated data in the star schema using staging tables. 
•	It is assumed that every table will have a directory in S3 and a new file will be uploaded each time there’s an update in the data. There will be a new file in S3 location. 


### Part 6: Insights Generation with SQL Queries
Write SQL queries to extract insights from the transformed star schema.
Top Selling Products:
Find the top 10 best-selling products based on the total quantity sold. Include the product name, category, and the total quantity sold.

#### Monthly Sales Trend:
Determine the monthly sales trend by calculating the total sales revenue for each month in a given year. 
Sales Performance Comparison:
Compare the sales performance of different stores by calculating the total sales revenue for each store. Rank the stores based on their sales revenue.

#### Seasonal Sales Analysis:
Analyze the seasonal sales patterns by calculating the total sales revenue for each quarter of a given year. Identify the quarter with the highest sales revenue.
Average Order Value:
Calculate the average order value by dividing the total sales revenue by the total number of orders. Analyze the average order value trends over time and identify any significant changes.

#### Store Location Analysis:
Analyze the sales performance based on store location by calculating the total sales revenue for each city or state

### Part 7 : Create a Data Pipeline to automate above tasks 



# Solution:

**Dimesional Modelling Flow Diagram**


![Flow Diagram](https://github.com/SaadAhmedWaqar/Data-Warehousing-Redshift/assets/105427072/12932ef0-4a52-4c31-a190-910439385980)




