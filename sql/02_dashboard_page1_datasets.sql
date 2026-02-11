-- ============================================================
-- Olist Ecommerce Lakehouse Analytics (Databricks SQL)
-- Page 1: Overview — Revenue & Delivery Experience
-- Uses global dashboard date-range parameter:
--      purchase_date BETWEEN :p_date.min AND :p_date.max
-- Source: workspace.olist_curated.analytics_orders_with_date
-- ============================================================


-- ------------------------------------------------------------
-- Dataset: Key Performance Metrics Overview
-- Used for KPI tiles:
--      Total Orders, Revenue (BRL), Average Order Value (BRL),
--      Average Review Score, Avg Delivery Time (Days), Late Delivery Rate
-- ------------------------------------------------------------
SELECT
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(item_revenue) AS revenue_brl,
  SUM(item_revenue) / COUNT(DISTINCT order_id) AS avg_order_value_brl,
  AVG(review_score) AS avg_review_score,
  AVG(delivery_time_days) AS avg_delivery_time_days,
  AVG(CASE WHEN delivery_delay_days > 0 THEN 1 ELSE 0 END) AS late_delivery_rate
FROM workspace.olist_curated.analytics_orders_with_date
WHERE purchase_date BETWEEN :p_date.min AND :p_date.max;


-- ------------------------------------------------------------
-- Dataset: Monthly Revenue Insights
-- Chart: Monthly Revenue Trend (line)
-- ------------------------------------------------------------
SELECT
  date_trunc('month', purchase_date) AS month,
  SUM(item_revenue) AS revenue,
  COUNT(DISTINCT order_id) AS orders
FROM workspace.olist_curated.analytics_orders_with_date
WHERE purchase_date BETWEEN :p_date.min AND :p_date.max
GROUP BY 1
ORDER BY 1;


-- ------------------------------------------------------------
-- Dataset: Category Performance Insights
-- Chart: Top 10 Categories by Revenue (bar)
-- ------------------------------------------------------------
SELECT
  product_category,
  SUM(item_revenue) AS revenue
FROM workspace.olist_curated.analytics_orders_with_date
WHERE purchase_date BETWEEN :p_date.min AND :p_date.max
GROUP BY 1
ORDER BY revenue DESC
LIMIT 10;


-- ------------------------------------------------------------
-- Dataset: Delay and Review Trends Data
-- Chart: Average Review Score by Delivery Delay (bar)
-- ------------------------------------------------------------
SELECT
  CASE
    WHEN delivery_delay_days IS NULL THEN 'unknown'
    WHEN delivery_delay_days <= 0 THEN 'on_time_or_early'
    WHEN delivery_delay_days BETWEEN 1 AND 3 THEN '1-3 days late'
    WHEN delivery_delay_days BETWEEN 4 AND 7 THEN '4-7 days late'
    ELSE '8+ days late'
  END AS delay_bucket,
  COUNT(*) AS items,
  AVG(review_score) AS avg_review
FROM workspace.olist_curated.analytics_orders_with_date
WHERE purchase_date BETWEEN :p_date.min AND :p_date.max
GROUP BY 1;
