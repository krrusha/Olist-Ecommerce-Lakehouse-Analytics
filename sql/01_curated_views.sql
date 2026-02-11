-- ============================================================
-- Olist Ecommerce Lakehouse Analytics (Databricks SQL)
--
-- Purpose:
--   Build the curated layer (clean tables + analytics-ready table)
--   and the dashboard-ready view: analytics_orders_with_date
--
-- Target schema:
--   workspace.olist_curated
--
-- Source schema (raw tables you loaded):
--   workspace.olist_lakehouse
-- ============================================================


-- ------------------------------------------------------------
-- (Optional) Create curated schema (safe if it already exists)
-- ------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS workspace.olist_curated
COMMENT 'Curated tables + views for Olist dashboard';


-- ------------------------------------------------------------
-- 1) Clean Orders
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE workspace.olist_curated.clean_orders AS
SELECT
  order_id,
  customer_id,
  order_status,
  to_timestamp(order_purchase_timestamp) AS purchase_ts,
  to_timestamp(order_delivered_customer_date) AS delivered_ts,
  to_timestamp(order_estimated_delivery_date) AS estimated_delivery_ts
FROM workspace.olist_lakehouse.olist_orders_dataset;


-- ------------------------------------------------------------
-- 2) Clean Order Items (adds item_revenue)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE workspace.olist_curated.clean_order_items AS
SELECT
  order_id,
  order_item_id,
  product_id,
  seller_id,
  CAST(price AS DOUBLE) AS price,
  CAST(freight_value AS DOUBLE) AS freight_value,
  CAST(price AS DOUBLE) + CAST(freight_value AS DOUBLE) AS item_revenue
FROM workspace.olist_lakehouse.olist_order_items_dataset;


-- ------------------------------------------------------------
-- 3) Clean Products + Category Translation (PT -> EN)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE workspace.olist_curated.clean_products AS
SELECT
  p.product_id,
  COALESCE(t.product_category_name_english, p.product_category_name) AS product_category
FROM workspace.olist_lakehouse.olist_products_dataset p
LEFT JOIN workspace.olist_lakehouse.product_category_name_translation t
  ON t.product_category_name = p.product_category_name;


-- ------------------------------------------------------------
-- 4) Clean Customers
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE workspace.olist_curated.clean_customers AS
SELECT
  customer_id,
  customer_city,
  customer_state
FROM workspace.olist_lakehouse.olist_customers_dataset;


-- ------------------------------------------------------------
-- 5) Clean Sellers
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE workspace.olist_curated.clean_sellers AS
SELECT
  seller_id,
  seller_city,
  seller_state
FROM workspace.olist_lakehouse.olist_sellers_dataset;


-- ------------------------------------------------------------
-- 6) Clean Reviews (1 row per order_id)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE workspace.olist_curated.clean_reviews AS
SELECT
  order_id,
  MAX(review_score) AS review_score
FROM workspace.olist_lakehouse.olist_order_reviews_dataset
GROUP BY order_id;


-- ------------------------------------------------------------
-- 7) Analytics-ready table (joins everything)
--    NOTE: delivery_delay_days definition matches your dashboard logic
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE workspace.olist_curated.analytics_orders AS
SELECT
  oi.order_id,
  oi.order_item_id,
  o.customer_id,
  oi.product_id,
  oi.seller_id,

  o.order_status,
  o.purchase_ts,
  o.delivered_ts,
  o.estimated_delivery_ts,

  oi.price,
  oi.freight_value,
  oi.item_revenue,

  c.customer_city,
  c.customer_state,
  s.seller_city,
  s.seller_state,
  p.product_category,
  r.review_score,

  datediff(o.delivered_ts, o.purchase_ts) AS delivery_time_days,
  datediff(o.delivered_ts, o.estimated_delivery_ts) AS delivery_delay_days
FROM workspace.olist_curated.clean_order_items oi
LEFT JOIN workspace.olist_curated.clean_orders o
  ON o.order_id = oi.order_id
LEFT JOIN workspace.olist_curated.clean_customers c
  ON c.customer_id = o.customer_id
LEFT JOIN workspace.olist_curated.clean_sellers s
  ON s.seller_id = oi.seller_id
LEFT JOIN workspace.olist_curated.clean_products p
  ON p.product_id = oi.product_id
LEFT JOIN workspace.olist_curated.clean_reviews r
  ON r.order_id = oi.order_id;


-- ------------------------------------------------------------
-- 8) (Optional) Optimize for faster dashboard queries
--    If OPTIMIZE isn't available on your tier, you can remove this.
-- ------------------------------------------------------------
OPTIMIZE workspace.olist_curated.analytics_orders
ZORDER BY (order_id, product_id, customer_id);


-- ------------------------------------------------------------
-- 9) Dashboard-ready view: adds purchase_date (DATE)
--    This is what Page 1 + Page 2 dashboard queries use.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW workspace.olist_curated.analytics_orders_with_date AS
SELECT
  *,
  to_date(purchase_ts) AS purchase_date
FROM workspace.olist_curated.analytics_orders
WHERE purchase_ts IS NOT NULL;
