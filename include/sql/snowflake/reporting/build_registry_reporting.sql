INSERT OVERWRITE INTO registry_reporting
SELECT
    EXTRACT(YEAR FROM crv.visit_date) AS year,
    EXTRACT(QUARTER FROM crv.visit_date) AS quarter,
    EXTRACT(MONTH FROM crv.visit_date) AS month,
    EXTRACT(WEEK FROM crv.visit_date) AS week,
    EXTRACT(DAY FROM crv.visit_date) AS day,
    cust.name,
    crv.visit_date,
    crv.visitors,
    crv.page_visits,
    LAG(crv.visitors) OVER(
        PARTITION BY cust.name
        ORDER BY crv.visit_date
    ) AS previous_daily_visitor_count,
    LAG(crv.page_visits) OVER(
        PARTITION BY cust.name
        ORDER BY crv.visit_date
    ) AS previous_daily_page_visits,
    SUM(crv.visitors) OVER(
        PARTITION BY
            cust.name,
            EXTRACT(YEAR FROM crv.visit_date)
        ORDER BY crv.visit_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_daily_visitors,
    SUM(crv.page_visits) OVER(
        PARTITION BY
            cust.name,
            EXTRACT(YEAR FROM crv.visit_date)
        ORDER BY crv.visit_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_daily_page_visits,
    SUM(crv.page_visits) OVER(
        PARTITION BY cust.name,
        EXTRACT(YEAR FROM crv.visit_date)
    ) AS year_total_page_visits,
    SUM(crv.visitors) OVER(
        PARTITION BY cust.name,
        EXTRACT(YEAR FROM crv.visit_date)
    ) AS year_total_visitors,
    SUM(crv.visitors) OVER(
        PARTITION BY cust.name,
        EXTRACT(QUARTER FROM crv.visit_date)
    ) AS quarter_total_visitors,
    SUM(crv.page_visits) OVER(
        PARTITION BY cust.name,
        EXTRACT(QUARTER FROM crv.visit_date)
    ) AS quarter_total_page_visits,
    SUM(crv.visitors) OVER(
        PARTITION BY cust.name,
        EXTRACT(MONTH FROM crv.visit_date)
    ) AS month_total_visitors,
    SUM(crv.page_visits) OVER(
        PARTITION BY cust.name,
        EXTRACT(MONTH FROM crv.visit_date)
    ) AS month_total_page_visits,
    SUM(crv.visitors) OVER(
        PARTITION BY cust.name,
        EXTRACT(WEEK FROM crv.visit_date)
    ) AS week_total_visitors,
    SUM(crv.page_visits) OVER(
        PARTITION BY cust.name,
        EXTRACT(WEEK FROM crv.visit_date)
    ) AS week_total_page_visits,
    SUM(crv.visitors) OVER(PARTITION BY cust.name) AS total_visitors,
    SUM(crv.page_visits) OVER(PARTITION BY cust.name) AS total_page_visits
FROM customers cust
   JOIN customer_registry_visits crv
  USING (name)
