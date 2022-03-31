-- Big project for SQL


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT
	format_date("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
	SUM(totals.visits) AS visits,
	SUM(totals.pageviews) AS pageviews,
	SUM(totals.transactions) AS transactions,
	SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
Where _table_suffix BETWEEN '20170101' AND '20170331'
GROUP BY month


-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT
	trafficSource.source AS source,
	COUNT ( trafficSource.source ) AS total_visits,
	SUM ( totals.bounces ) AS total_no_of_bounces,
	( ( SUM ( totals.bounces ) / COUNT ( trafficSource.source ) ) * 100 ) AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _table_suffix BETWEEN '20170701' AND '20170731'
GROUP BY source
ORDER BY total_visits DESC
LIMIT 4


-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL
with month_data as(
SELECT
  "Month" as time_type,
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
),

week_data as(
SELECT
  "Week" as time_type,
  format_date("%Y%W", parse_date("%Y%m%d", date)) as date,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
)

select * from month_data
union all
select * from week_data


-- Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
WITH pageviews_purchase AS (
    SELECT
    month,
	( SUM(pagesviews) / COUNT(users) ) AS avg_pageviews_purchase
FROM (
	SELECT
        format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
		fullVisitorId AS users,
		SUM(totals.pageviews) AS pagesviews
	FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
	WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170731'
	AND totals.transactions >=1
	GROUP By month, users )
GROUP BY month
),

pageviews_non_purchase AS (
    SELECT
    month,
	( SUM(pagesviews) / COUNT(users) ) AS avg_pageviews_non_purchase
FROM (
	SELECT
        format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
		fullVisitorId AS users,
		SUM(totals.pageviews) AS pagesviews
	FROM`bigquery-public-data.google_analytics_sample.ga_sessions_*`
	WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170731'
	AND totals.transactions IS NULL
	GROUP BY month, users )
GROUP BY month
)

SELECT *
FROM pageviews_purchase
JOIN pageviews_non_purchase USING (month)
ORDER BY month

-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    sum(totals.transactions)/count(distinct fullvisitorid) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where  totals.transactions>=1
group by month;


-- Query 06: Average amount of money spent per session
#standardSQL
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    ((sum(totals.totalTransactionRevenue)/sum(totals.visits))/power(10,6)) as avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where  totals.transactions is not null
group by month;


-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)
#standardSQL
SELECT
    product.v2ProductName AS other_purchased_products,
    sum(product.productQuantity) as quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
WHERE _table_suffix BETWEEN '20170701' AND '20170731'
  AND fullVisitorId IN (
    SELECT fullVisitorId
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST (hits) hits,
        UNNEST (hits.product) product
    WHERE _table_suffix BETWEEN '20170701' AND '20170731'
        AND product.v2ProductName="YouTube Men's Vintage Henley"
        AND totals.transactions >= 1
        AND product.productRevenue IS NOT NULL
    GROUP BY fullVisitorId )
 AND product.productRevenue IS NOT NULL
 AND product.v2ProductName !="YouTube Men's Vintage Henley"
GROUP BY other_purchased_products
ORDER BY quantity DESC;

SELECT fullVisitorId
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST (hits) hits,
        UNNEST (hits.product) product
    WHERE _table_suffix BETWEEN '20170701' AND '20170731'
        AND product.v2ProductName="YouTube Men's Vintage Henley"
        AND totals.transactions >= 1
        AND product.productRevenue IS NOT NULL
    GROUP BY fullVisitorId


-- Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
with
product_detail as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '2'
GROUP BY 1
--ORDER BY 4 DESC
),

add_to_cart as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '3'
GROUP BY 1
--ORDER BY 4 DESC
),

purchase as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '6'
group by 1
)

select
    pd.*,
    num_addtocart,
    num_purchase,
    round(num_addtocart*100/num_product_view,2) as add_to_cart_rate,
    round(num_purchase*100/num_product_view,2) as purchase_rate
from product_detail pd
join add_to_cart a on pd.month = a.month
join purchase p on pd.month = p.month
order by pd.month
