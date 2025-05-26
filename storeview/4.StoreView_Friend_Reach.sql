-- 4. StoreView_Friend_Reach
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Friend_Reach;
CREATE TABLE prd_crm.stage.StoreView_Friend_Reach AS
WITH sa_table AS (
    SELECT DISTINCT user_id, name, local_name, store_nbr, counter_type_code, region, region_datail
    FROM prd_crm.dlabs.store_view_sa_customer
    WHERE sa_status IN ('0', '1', '4')
        AND local_name NOT IN ('TW EDA (Temp Store)_OCF23')
),
max_year AS (
    SELECT MIN(add_date) AS fy_start_date,
        MAX(CAST(20 || SUBSTRING(fisc_year, 3, 2) AS INT)) AS year
    FROM prd_crm.dlabs.store_view_sa_customer
    WHERE CAST(20 || SUBSTRING(fisc_year, 3, 2) AS INT) IN (
            SELECT CAST(20 || SUBSTRING(MAX(fisc_year), 3, 2) AS INT) AS year_max
            FROM prd_crm.dlabs.store_view_sa_customer
        )
),
-- 1. calculate the sa sales with adding info list, successful sale, consumotion amount
add_total AS (
    SELECT add_date, user_id, card_type_new, COUNT(DISTINCT work_external_user_id) AS people
    FROM (
        SELECT work_external_user_id, user_id, name, add_date, card_type_new
        FROM prd_crm.dlabs.store_view_sa_customer
        WHERE user_id IS NOT NULL
            AND add_rank = 1
            AND add_date > '1970-01-01'
        GROUP BY work_external_user_id, user_id, name, add_date, card_type_new
    )
    GROUP BY add_date, user_id, card_type_new
),
successful_sales_amount AS (
    SELECT txn_date, user_id_txn, card_type_new, SUM(demand) AS successful_sales_amount
    FROM (
        SELECT work_external_user_id, customer_id, card_type_new, txn_nbr, txn_date, user_id_txn, demand
        FROM prd_crm.dlabs.store_view_sa_customer
        WHERE user_id_txn IS NOT NULL
        GROUP BY work_external_user_id, customer_id, card_type_new, txn_nbr, txn_date, user_id_txn, demand
    )
    GROUP BY txn_date, user_id_txn, card_type_new
),
consumption_amount AS (
    SELECT txn_date, user_id, card_type_new, SUM(demand) AS consumption_amount
    FROM (
        SELECT work_external_user_id, user_id, name, txn_date, txn_nbr, card_type_new, demand
        FROM prd_crm.dlabs.store_view_sa_customer
        WHERE user_id IS NOT NULL
        GROUP BY work_external_user_id, user_id, name, txn_date, txn_nbr, card_type_new, demand
    )
    GROUP BY txn_date, user_id, card_type_new
),
amout_summary_sep AS (
    SELECT COALESCE(t1.txn_date, t2.txn_date) AS txn_date,
        COALESCE(t1.user_id_txn, t2.user_id) AS user_id,
        COALESCE(t1.card_type_new, t2.card_type_new) AS card_type_new,
        t1.successful_sales_amount,
        t2.consumption_amount
    FROM successful_sales_amount t1
    FULL OUTER JOIN consumption_amount t2
        ON t1.txn_date = t2.txn_date
            AND t1.user_id_txn = t2.user_id
            AND t1.card_type_new = t2.card_type_new
    WHERE t1.txn_date IS NOT NULL OR t2.txn_date IS NOT NULL
),
-- 2. Reciver Id and info list
receiver AS (
    SELECT t1.work_external_user_id, t2.openid, t1.customer_id, t1.card_type_new
    FROM (
        SELECT work_external_user_id, card_type_new, customer_id
        FROM prd_crm.dlabs.store_view_sa_customer
        GROUP BY work_external_user_id, card_type_new, customer_id
    ) t1
    LEFT JOIN (
        SELECT openid, id, customer_id
        FROM prd_c360.landing.weclient_wx_user_corp_external
        GROUP BY openid, id, customer_id
    ) t2
        ON t1.work_external_user_id = t2.id
),
-- 3. Filter the msg with customer and between 10:00-22:00
filter_msg AS (
    SELECT sender_external_userid AS sender, receiver_external_userid AS receiver,
        CAST(msgtime AS TIMESTAMP) AS msgtime, DATE(CAST(msgtime AS TIMESTAMP)) AS msg_date
    FROM prd_crm.cdlcrm.cdl_customer_sa_wecom_chat, max_year
    WHERE brand_code IN ('Coach')
        AND sender_external_userid IN (
            SELECT user_id
            FROM sa_table
            GROUP BY user_id
        )
        AND receiver_external_userid NOT IN (
            SELECT user_id
            FROM sa_table
            GROUP BY user_id
        )
        AND SPLIT_PART(msgtime, ' ', 2) BETWEEN '10:00:00' AND '22:00:00'
        AND DATE(CAST(msgtime AS TIMESTAMP)) >= fy_start_date
    GROUP BY sender_external_userid, receiver_external_userid, msgtime, msg_date
),
-- 4. >60 considered as 1 session or only 1 message without any feedback is also the 1 session
session_markers AS (
    SELECT sender, receiver, msg_date, msgtime,
        CASE
            WHEN DATEDIFF(minute, LAG(msgtime) OVER (PARTITION BY sender, receiver, msg_date ORDER BY msgtime), msgtime) > 60
                OR LAG(msgtime) OVER (PARTITION BY sender, receiver, msg_date ORDER BY msgtime) IS NULL THEN 1
            ELSE 0
        END AS is_new_session
    FROM filter_msg
),
-- 5. find fiscal date of the msg_date
fiscal_date AS (
    SELECT t1.msg_date,
        t3.time_period AS fiscal_week,
        t4.time_period AS fiscal_month,
        t5.time_period AS fiscal_quarter,
        t6.time_period AS fiscal_year
    FROM (
        SELECT msg_date
        FROM session_markers
        GROUP BY msg_date
    ) t1
    LEFT JOIN prd_crm.dlabs.dlab_calendar t3
        ON t1.msg_date BETWEEN t3.start_date AND t3.end_date
        AND t3.time_period_code IN ('Week')
    LEFT JOIN prd_crm.dlabs.dlab_calendar t4
        ON t1.msg_date BETWEEN t4.start_date AND t4.end_date
        AND t4.time_period_code IN ('Month')
    LEFT JOIN prd_crm.dlabs.dlab_calendar t5
        ON t1.msg_date BETWEEN t5.start_date AND t5.end_date
        AND t5.time_period_code IN ('Quarter')
    LEFT JOIN prd_crm.dlabs.dlab_calendar t6
        ON t1.msg_date BETWEEN t6.start_date AND t6.end_date
        AND t6.time_period_code IN ('Year')
),
session_counts AS (
    SELECT t1.msg_date, t4.fiscal_week, t4.fiscal_month, t4.fiscal_quarter, t4.fiscal_year,
        t2.user_id AS sender, t2.name, t2.local_name, t2.store_nbr, t2.counter_type_code, t2.region, t2.region_datail,
        t3.work_external_user_id AS receiver, t3.card_type_new,
        SUM(t1.is_new_session) AS is_session,
        CASE WHEN SUM(t1.is_new_session) >= 2 THEN 1 ELSE 0 END AS is_quality_session
    FROM session_markers t1
    LEFT JOIN sa_table t2
        ON t1.sender = t2.user_id
    LEFT JOIN receiver t3
        ON t1.receiver = t3.openid
    LEFT JOIN fiscal_date t4
        ON t1.msg_date = t4.msg_date
    GROUP BY t1.msg_date, t4.fiscal_week, t4.fiscal_month, t4.fiscal_quarter, t4.fiscal_year,
        t2.user_id, t2.name, t2.local_name, t2.store_nbr, t2.counter_type_code, t2.region, t2.region_datail,
        t3.work_external_user_id, t3.card_type_new
),
current_period AS (
    SELECT fiscal_year AS current_year,
        MAX(fiscal_quarter) AS current_quarter,
        MAX(fiscal_month) AS current_month,
        MAX(fiscal_week) AS current_week,
        MAX(msg_date) AS current_date
    FROM session_counts
    GROUP BY fiscal_year
),
base_summary AS (
    SELECT t1.organize_type, t1.organize, t1.store_name, t1.year, t1.time_period, t1.ordinal_number, t1.type,
        SUM(t1.people) AS people,
        SUM(t1.commnunation_count) AS commnunation_count,
        SUM(t1.commnunation_people_count) AS commnunation_people_count,
        SUM(t1.quality_commnunation_count) AS quality_commnunation_count,
        SUM(t1.quality_commnunation_people_count) AS quality_commnunation_people_count,
        SUM(t1.successful_sales_amount) AS successful_sales_amount,
        SUM(t1.consumption_amount) AS consumption_amount
    FROM (
        -- 门店汇总
        SELECT 4 AS organize_type, store_nbr AS organize, local_name AS store_name, fiscal_year AS year,
            CASE time_period
                WHEN 'DTD' THEN 'DTD'
                WHEN 'WTD' THEN 'WTD'
                WHEN 'MTD' THEN 'MTD'
                WHEN 'QTD' THEN 'QTD'
                WHEN 'YTD' THEN 'YTD'
            END AS time_period,
            CASE time_period
                WHEN 'DTD' THEN REPLACE(msg_date, '-', '')
                WHEN 'WTD' THEN SUBSTRING(fiscal_week, 5, 2)
                WHEN 'MTD' THEN SUBSTRING(fiscal_month, 5, 2)
                WHEN 'QTD' THEN SUBSTRING(fiscal_quarter, 5, 2)
                WHEN 'YTD' THEN CAST(fiscal_year AS VARCHAR)
            END AS ordinal_number,
            CASE card_type_new
                WHEN 'DIAMOND,' THEN 2
                WHEN 'GOLD' THEN 2
                WHEN 'Regular' THEN 2
                WHEN 'SILVER' THEN 2
                WHEN 'Non-Member' THEN 4
                ELSE 3
            END AS type,
            SUM(people) AS people,
            SUM(is_session) AS commnunation_count,
            COUNT(DISTINCT CASE WHEN is_session >= 1 THEN receiver END) AS commnunation_people_count,
            SUM(is_quality_session) AS quality_commnunation_count,
            COUNT(DISTINCT CASE WHEN is_quality_session = 1 THEN receiver END) AS quality_commnunation_people_count,
            SUM(t2.successful_sales_amount) AS successful_sales_amount,
            SUM(t2.consumption_amount) AS consumption_amount
        FROM session_counts t1
        LEFT JOIN amout_summary_sep t2
            ON t1.msg_date = t2.txn_date
                AND t1.sender = t2.user_id
                AND t1.card_type_new = t2.card_type_new
        LEFT JOIN add_total t3
            ON t3.add_date <= t1.msg_date
                AND t1.sender = t3.user_id
                AND t1.card_type_new = t3.card_type_new
        GROUP BY store_nbr, local_name, fiscal_year, time_period, msg_date, fiscal_week, fiscal_month, fiscal_quarter,
            card_type_new

        UNION ALL

        -- 分区汇总
        SELECT 3 AS organize_type, region_datail AS organize, '' AS store_name, fiscal_year AS year,
            CASE time_period
                WHEN 'DTD' THEN 'DTD'
                WHEN 'WTD' THEN 'WTD'
                WHEN 'MTD' THEN 'MTD'
                WHEN 'QTD' THEN 'QTD'
                WHEN 'YTD' THEN 'YTD'
            END AS time_period,
            CASE time_period
                WHEN 'DTD' THEN REPLACE(msg_date, '-', '')
                WHEN 'WTD' THEN SUBSTRING(fiscal_week, 5, 2)
                WHEN 'MTD' THEN SUBSTRING(fiscal_month, 5, 2)
                WHEN 'QTD' THEN SUBSTRING(fiscal_quarter, 5, 2)
                WHEN 'YTD' THEN CAST(fiscal_year AS VARCHAR)
            END AS ordinal_number,
            CASE card_type_new
                WHEN 'DIAMOND,' THEN 2
                WHEN 'GOLD' THEN 2
                WHEN 'Regular' THEN 2
                WHEN 'SILVER' THEN 2
                WHEN 'Non-Member' THEN 4
                ELSE 3
            END AS type,
            SUM(people) AS people,
            SUM(is_session) AS commnunation_count,
            COUNT(DISTINCT CASE WHEN is_session >= 1 THEN receiver END) AS commnunation_people_count,
            SUM(is_quality_session) AS quality_commnunation_count,
            COUNT(DISTINCT CASE WHEN is_quality_session = 1 THEN receiver END) AS quality_commnunation_people_count,
            SUM(t2.successful_sales_amount) AS successful_sales_amount,
            SUM(t2.consumption_amount) AS consumption_amount
        FROM session_counts t1
        LEFT JOIN amout_summary_sep t2
            ON t1.msg_date = t2.txn_date
                AND t1.sender = t2.user_id
                AND t1.card_type_new = t2.card_type_new
        LEFT JOIN add_total t3
            ON t3.add_date <= t1.msg_date
                AND t1.sender = t3.user_id
                AND t1.card_type_new = t3.card_type_new
        GROUP BY region_datail, fiscal_year, time_period, msg_date, fiscal_week, fiscal_month, fiscal_quarter, card_type_new

        UNION ALL

        -- 大区汇总
        SELECT 2 AS organize_type, region AS organize, '' AS store_name, fiscal_year AS year,
            CASE time_period
                WHEN 'DTD' THEN 'DTD'
                WHEN 'WTD' THEN 'WTD'
                WHEN 'MTD' THEN 'MTD'
                WHEN 'QTD' THEN 'QTD'
                WHEN 'YTD' THEN 'YTD'
            END AS time_period,
            CASE time_period
                WHEN 'DTD' THEN REPLACE(msg_date, '-', '')
                WHEN 'WTD' THEN SUBSTRING(fiscal_week, 5, 2)
                WHEN 'MTD' THEN SUBSTRING(fiscal_month, 5, 2)
                WHEN 'QTD' THEN SUBSTRING(fiscal_quarter, 5, 2)
                WHEN 'YTD' THEN CAST(fiscal_year AS VARCHAR)
            END AS ordinal_number,
            CASE card_type_new
                WHEN 'DIAMOND,' THEN 2
                WHEN 'GOLD' THEN 2
                WHEN 'Regular' THEN 2
                WHEN 'SILVER' THEN 2
                WHEN 'Non-Member' THEN 4
                ELSE 3
            END AS type,
            SUM(people) AS people,
            SUM(is_session) AS commnunation_count,
            COUNT(DISTINCT CASE WHEN is_session >= 1 THEN receiver END) AS commnunation_people_count,
            SUM(is_quality_session) AS quality_commnunation_count,
            COUNT(DISTINCT CASE WHEN is_quality_session = 1 THEN receiver END) AS quality_commnunation_people_count,
            SUM(t2.successful_sales_amount) AS successful_sales_amount,
            SUM(t2.consumption_amount) AS consumption_amount
        FROM session_counts t1
        LEFT JOIN amout_summary_sep t2
            ON t1.msg_date = t2.txn_date
                AND t1.sender = t2.user_id
                AND t1.card_type_new = t2.card_type_new
        LEFT JOIN add_total t3
            ON t3.add_date <= t1.msg_date
                AND t1.sender = t3.user_id
                AND t1.card_type_new = t3.card_type_new
        GROUP BY region, fiscal_year, time_period, msg_date, fiscal_week, fiscal_month, fiscal_quarter, card_type_new

        UNION ALL

        -- 渠道汇总
        SELECT 1 AS organize_type,
            CASE counter_type_code
                WHEN 'Factory' THEN 'PRC Outlet Total'
                WHEN 'Regular' THEN 'PRC Retail Total'
                ELSE counter_type_code
            END AS organize,
            '' AS store_name, fiscal_year AS year,
            CASE time_period
                WHEN 'DTD' THEN 'DTD'
                WHEN 'WTD' THEN 'WTD'
                WHEN 'MTD' THEN 'MTD'
                WHEN 'QTD' THEN 'QTD'
                WHEN 'YTD' THEN 'YTD'
            END AS time_period,
            CASE time_period
                WHEN 'DTD' THEN REPLACE(msg_date, '-', '')
                WHEN 'WTD' THEN SUBSTRING(fiscal_week, 5, 2)
                WHEN 'MTD' THEN SUBSTRING(fiscal_month, 5, 2)
                WHEN 'QTD' THEN SUBSTRING(fiscal_quarter, 5, 2)
                WHEN 'YTD' THEN CAST(fiscal_year AS VARCHAR)
            END AS ordinal_number,
            CASE card_type_new
                WHEN 'DIAMOND,' THEN 2
                WHEN 'GOLD' THEN 2
                WHEN 'Regular' THEN 2
                WHEN 'SILVER' THEN 2
                WHEN 'Non-Member' THEN 4
                ELSE 3
            END AS type,
            SUM(people) AS people,
            SUM(is_session) AS commnunation_count,
            COUNT(DISTINCT CASE WHEN is_session >= 1 THEN receiver END) AS commnunation_people_count,
            SUM(is_quality_session) AS quality_commnunation_count,
            COUNT(DISTINCT CASE WHEN is_quality_session = 1 THEN receiver END) AS quality_commnunation_people_count,
            SUM(t2.successful_sales_amount) AS successful_sales_amount,
            SUM(t2.consumption_amount) AS consumption_amount
        FROM session_counts t1
        LEFT JOIN amout_summary_sep t2
            ON t1.msg_date = t2.txn_date
                AND t1.sender = t2.user_id
                AND t1.card_type_new = t2.card_type_new
        LEFT JOIN add_total t3
            ON t3.add_date <= t1.msg_date
                AND t1.sender = t3.user_id
                AND t1.card_type_new = t3.card_type_new
        GROUP BY counter_type_code, fiscal_year, time_period, msg_date, fiscal_week, fiscal_month, fiscal_quarter, card_type_new

        UNION ALL

        -- 总计汇总
        SELECT 0 AS organize_type, '' AS organize, '' AS store_name, fiscal_year AS year,
            CASE time_period
                WHEN 'DTD' THEN 'DTD'
                WHEN 'WTD' THEN 'WTD'
                WHEN 'MTD' THEN 'MTD'
                WHEN 'QTD' THEN 'QTD'
                WHEN 'YTD' THEN 'YTD'
            END AS time_period,
            CASE time_period
                WHEN 'DTD' THEN REPLACE(msg_date, '-', '')
                WHEN 'WTD' THEN SUBSTRING(fiscal_week, 5, 2)
                WHEN 'MTD' THEN SUBSTRING(fiscal_month, 5, 2)
                WHEN 'QTD' THEN SUBSTRING(fiscal_quarter, 5, 2)
                WHEN 'YTD' THEN CAST(fiscal_year AS VARCHAR)
            END AS ordinal_number,
            CASE card_type_new
                WHEN 'DIAMOND,' THEN 2
                WHEN 'GOLD' THEN 2
                WHEN 'Regular' THEN 2
                WHEN 'SILVER' THEN 2
                WHEN 'Non-Member' THEN 4
                ELSE 3
            END AS type,
            SUM(people) AS people,
            SUM(is_session) AS commnunation_count,
            COUNT(DISTINCT CASE WHEN is_session >= 1 THEN receiver END) AS commnunation_people_count,
            SUM(is_quality_session) AS quality_commnunation_count,
            COUNT(DISTINCT CASE WHEN is_quality_session = 1 THEN receiver END) AS quality_commnunation_people_count,
            SUM(t2.successful_sales_amount) AS successful_sales_amount,
            SUM(t2.consumption_amount) AS consumption_amount
        FROM session_counts t1
        LEFT JOIN amout_summary_sep t2
            ON t1.msg_date = t2.txn_date
                AND t1.sender = t2.user_id
                AND t1.card_type_new = t2.card_type_new
        LEFT JOIN add_total t3
            ON t3.add_date <= t1.msg_date
                AND t1.sender = t3.user_id
                AND t1.card_type_new = t3.card_type_new
        GROUP BY fiscal_year, time_period, msg_date, fiscal_week, fiscal_month, fiscal_quarter, card_type_new
    ) t1
    GROUP BY t1.organize_type, t1.organize, t1.store_name, t1.year, t1.time_period, t1.ordinal_number, t1.type
),
summary_store AS (
    SELECT t.organize_type, t.organize, t.store_name, t.year, t.time_period, t.ordinal_number, t.type,
        t.people, t.commnunation_count, t.commnunation_people_count,
        t.quality_commnunation_count, t.quality_commnunation_people_count,
        t.successful_sales_amount, t.consumption_amount
    FROM base_summary t
)
SELECT (SELECT MAX(add_date) FROM add_total) AS data_date, t1.*
FROM summary_store t1;
