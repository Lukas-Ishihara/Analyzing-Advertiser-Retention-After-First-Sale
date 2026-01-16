/* Project: Analyzing Advertiser Retention After First Sale
Dialect: PostgreSQL */

WITH add_row_number AS (
    SELECT
	    ad_id,
		sale_date,
	    ROW_NUMBER() OVER (
		    PARTITION BY ad_id
			ORDER BY sale_date ASC
		) AS  rn
	FROM runs
),
first_sale_data AS (
    SELECT
	    ad_id,
        sale_date AS first_sale_date
	FROM add_row_number
	WHERE rn = 1
),
second_sale_data AS (
    SELECT
	    ad_id,
        sale_date AS second_sale_date
	FROM add_row_number
	WHERE rn = 2
),
third_sale_data AS (
    SELECT
	    ad_id,
        sale_date AS third_sale_date
	FROM add_row_number
	WHERE rn = 3
),
combine_all_sales_data AS (
    SELECT
		DISTINCT r.ad_id,
	    DATE_TRUNC('month', fsd.first_sale_date) AS first_sale_month,
		fsd.first_sale_date,
		ssd.second_sale_date,
		tsd.third_sale_date
    FROM runs r
	LEFT JOIN first_sale_data fsd ON fsd.ad_id = r.ad_id
	LEFT JOIN second_sale_data ssd ON ssd.ad_id = r.ad_id
	LEFT JOIN third_sale_data tsd ON tsd.ad_id = r.ad_id
),
check_second AS (
    SELECT
        ad_id,
		first_sale_month,
		second_sale_date,
		third_sale_date,
		CASE
		    WHEN second_sale_date - first_sale_date < 28 THEN 1
		    ELSE 0
		END AS second_in_time
    FROM combine_all_sales_data
),
check_third AS (
    SELECT
	    ad_id,
		first_sale_month,
		second_in_time,
		CASE
		    WHEN third_sale_date - second_sale_date < 28
			    AND second_in_time = 1
				THEN 1
		    ELSE 0
		END AS third_in_time
    FROM check_second
),
count_advertisers AS (
    SELECT
	    first_sale_month,
        SUM(second_in_time) AS count_second_in_time,
		SUM(third_in_time) AS count_third_in_time,
		COUNT(*) AS total_advertisers
	FROM check_third
	GROUP BY first_sale_month
)

SELECT
    TO_CHAR(first_sale_month, 'YYYY-MM') AS year_month,
	total_advertisers,
    ROUND(
	    100.0 * count_second_in_time / total_advertisers
	, 2) AS percent_second_sale,
	ROUND(
	    100.0 * count_third_in_time / total_advertisers
	, 2) AS percent_third_sale
FROM count_advertisers

ORDER BY year_month;
