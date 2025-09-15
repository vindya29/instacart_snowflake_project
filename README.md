# instacart_snowflake_project
Snowflake analytics project for Instacart dataset



📊 Instacart Sales Analytics Dashboard
Welcome to the Instacart Sales Analytics Dashboard project! 🚀
This project integrates Snowflake as the data warehouse and Tableau for visualization to analyze customer purchasing behavior, product demand, and sales trends.
🔎 Project Overview
Objective: Provide insights into Instacart’s order patterns, customer behavior, and product performance.
Data Source: Instacart dataset (Kaggle).
Tech Stack:
Snowflake – Cloud Data Warehouse
Tableau – Visualization & Dashboards
SQL – Data Cleaning & Modeling
⚙️ Workflow
Data Ingestion
Load Instacart dataset into Snowflake.
Organize raw data into structured tables (orders, products, users, etc.).
Data Cleaning & Transformation
Create cleaned models (e.g., ORDERS_CLEAN, PRODUCTS_CLEAN, USER_STATS).
Handle missing values, duplicates, and standardize formats.
Data Modeling
Join fact & dimension tables.
Build analytical datasets for Tableau connection.
Visualization in Tableau
Publish dashboards showing:
🛒 Top Products Ordered
👥 User Segments (New vs Returning customers)
⏰ Peak Ordering Times
💵 Revenue by Department
⭐ Product Reviews & Ratings
CREATE DATABASE instacart_db;
CREATE SCHEMA raw_data;
CREATE SCHEMA analytics;
