-- ============================================================
-- Olist Ecommerce Lakehouse Analytics (Databricks SQL)
-- Page 2: Operations & Geography — Fulfillment Hotspots
-- Uses global dashboard date-range parameter:
--      purchase_date BETWEEN :p_date.min AND :p_date.max
-- Source: workspace.olist_curated.analytics_orders_with_date
-- ============================================================


-- ------------------------------------------------------------
-- Dataset: Top States Revenue and Orders Summary
-- Chart: Top Customer States by Revenue
-- Tooltip: Revenue Share
-- ------------------------------------------------------------
WITH state_rev AS (
  SELECT
    customer_state,
    SUM(item_revenue) AS revenue_brl
  FROM workspace.olist_curated.analytics_orders_with_date
  WHERE purchase_date BETWEEN :p_date.min AND :p_date.max
  GROUP BY 1
),
tot AS (
  SELECT SUM(revenue_brl) AS total_revenue_brl
  FROM state_rev
)
SELECT
  s.customer_state,
  s.revenue_brl,
  s.revenue_brl / t.total_revenue_brl AS revenue_share
FROM state_rev s
CROSS JOIN tot t
ORDER BY s.revenue_brl DESC
LIMIT 10;


-- ------------------------------------------------------------
-- Dataset: Seller State Late Delivery Performance Analysis
-- Chart: Seller States with Highest Late Delivery Rate
-- Note: Filtered to states with meaningful volume (orders >= 300)
-- ------------------------------------------------------------
SELECT
  seller_state,
  COUNT(DISTINCT order_id) AS orders,
  AVG(CASE WHEN delivery_delay_days > 0 THEN 1 ELSE 0 END) AS late_delivery_rate
FROM workspace.olist_curated.analytics_orders_with_date
WHERE purchase_date BETWEEN :p_date.min AND :p_date.max
  AND delivered_ts IS NOT NULL
  AND estimated_delivery_ts IS NOT NULL
GROUP BY 1
HAVING orders >= 300
ORDER BY late_delivery_rate DESC
LIMIT 10;


-- ------------------------------------------------------------
-- Dataset: Statewise Order Delivery Delays and Revenue Analysis
-- Table: Worst Fulfillment Routes (Seller → Customer)
-- Adds impact columns:
--      est_late_orders, revenue_at_risk_brl
-- IMPORTANT: Keep ORDER BY to preserve "worst routes" ranking
-- ------------------------------------------------------------
SELECT
  seller_state,
  customer_state,
  COUNT(DISTINCT order_id) AS orders,
  AVG(CASE WHEN delivery_delay_days > 0 THEN 1 ELSE 0 END) AS late_delivery_rate,
  AVG(CASE WHEN delivery_delay_days > 0 THEN delivery_delay_days END) AS avg_late_days,
  SUM(item_revenue) AS revenue_brl,

  -- impact columns (do not affect ranking)
  COUNT(DISTINCT order_id) * AVG(CASE WHEN delivery_delay_days > 0 THEN 1 ELSE 0 END) AS est_late_orders,
  SUM(item_revenue) * AVG(CASE WHEN delivery_delay_days > 0 THEN 1 ELSE 0 END) AS revenue_at_risk_brl

FROM workspace.olist_curated.analytics_orders_with_date
WHERE purchase_date BETWEEN :p_date.min AND :p_date.max
  AND delivered_ts IS NOT NULL
  AND estimated_delivery_ts IS NOT NULL
GROUP BY 1,2
HAVING orders >= 300
ORDER BY late_delivery_rate DESC, orders DESC
LIMIT 10;


-- ------------------------------------------------------------
-- Dataset: Order Delivery Delay Group Summary
-- Chart: Delivery Delay Distribution (share of items)
-- ------------------------------------------------------------
WITH dist AS (
  SELECT
    CASE
      WHEN delivery_delay_days IS NULL THEN 'unknown'
      WHEN delivery_delay_days <= 0 THEN 'on_time_or_early'
      WHEN delivery_delay_days BETWEEN 1 AND 3 THEN '1-3 days late'
      WHEN delivery_delay_days BETWEEN 4 AND 7 THEN '4-7 days late'
      WHEN delivery_delay_days BETWEEN 8 AND 14 THEN '8-14 days late'
      ELSE '15+ days late'
    END AS delay_bucket,
    COUNT(*) AS items
  FROM workspace.olist_curated.analytics_orders_with_date
  WHERE purchase_date BETWEEN :p_date.min AND :p_date.max
  GROUP BY 1
),
tot AS (
  SELECT SUM(items) AS total_items
  FROM dist
)
SELECT
  d.delay_bucket,
  d.items,
  d.items / t.total_items AS pct_items
FROM dist d
CROSS JOIN tot t;


-- ------------------------------------------------------------
-- Dataset: Customer Experience Insights by State
-- Chart: Avg Review Score in Top Revenue States
-- Top 10 states are determined by revenue in the selected range
-- ------------------------------------------------------------
WITH top_states AS (
  SELECT customer_state
  FROM workspace.olist_curated.analytics_orders_with_date
  WHERE purchase_date BETWEEN :p_date.min AND :p_date.max
  GROUP BY 1
  ORDER BY SUM(item_revenue) DESC
  LIMIT 10
)
SELECT
  a.customer_state,
  AVG(a.review_score) AS avg_review_score,
  COUNT(DISTINCT a.order_id) AS orders
FROM workspace.olist_curated.analytics_orders_with_date a
JOIN top_states t
  ON a.customer_state = t.customer_state
WHERE a.purchase_date BETWEEN :p_date.min AND :p_date.max
GROUP BY 1
ORDER BY avg_review_score ASC;
