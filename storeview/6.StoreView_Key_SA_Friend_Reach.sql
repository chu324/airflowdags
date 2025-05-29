SET enable_result_cache_for_session TO ON;
-- 1. 创建基础中间表：财年首日
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_fy_firstday;
CREATE TABLE prd_crm.stage.StoreView_Key_SA_Friend_Reach_fy_firstday AS
SELECT 
  DATE(calendar_date) AS start_timewindow,
  fisc_year_num
FROM cdlcrm.cdl_mstr_calendar
WHERE fisc_year_day_num = 1
  AND fisc_year_num = (
    SELECT fisc_year_num 
    FROM cdlcrm.cdl_mstr_calendar 
    WHERE DATE(calendar_date) = DATE(GETDATE() + INTERVAL '8 hours')
  );

-- 2. SA基础信息表
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_sa_table;
CREATE TABLE prd_crm.stage.StoreView_Key_SA_Friend_Reach_sa_table
DISTKEY(user_id) SORTKEY(user_id) AS
SELECT DISTINCT 
  user_id, 
  name, 
  local_name, 
  store_nbr, 
  counter_type_code, 
  region, 
  region_datail,
  work_external_user_id
FROM prd_crm.dlabs.store_view_sa_customer
WHERE sa_status IN ('0','1','4')
  AND local_name != 'TW EDA (Temp Store)_OCF23'
  AND is_key_sa = '1';

-- 3. 财年日期范围表
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_max_year;
CREATE TABLE prd_crm.stage.StoreView_Key_SA_Friend_Reach_max_year AS
SELECT 
  MIN(add_date) AS fy_start_date,
  MAX(CAST(20 || SUBSTRING(fisc_year, 3, 2) AS INT)) AS year
FROM prd_crm.dlabs.store_view_sa_customer
WHERE CAST(20 || SUBSTRING(fisc_year, 3, 2) AS INT) = (
  SELECT CAST(20 || SUBSTRING(MAX(fisc_year), 3, 2) AS INT) 
  FROM prd_crm.dlabs.store_view_sa_customer
);

-- 4. 新增客户统计表
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_add_total;
CREATE TABLE prd_crm.stage.StoreView_Key_SA_Friend_Reach_add_total AS
SELECT 
  add_date,
  user_id,
  card_type_new,
  COUNT(DISTINCT work_external_user_id) AS people
FROM (
  SELECT
    work_external_user_id,
    user_id,
    name,
    add_date,
    card_type_new
  FROM prd_crm.dlabs.store_view_sa_customer 
  WHERE user_id IS NOT NULL 
    AND add_rank = 1
    AND is_key_sa = '1'
    AND add_date > '1970-01-01'
  GROUP BY 1,2,3,4,5
)
GROUP BY 1,2,3;

-- 5. 成功销售金额表
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_successful_sales_amount;
CREATE TABLE prd_crm.stage.StoreView_Key_SA_Friend_Reach_successful_sales_amount AS
SELECT
  txn_date,
  user_id_txn,
  card_type_new,
  SUM(demand) AS successful_sales_amount
FROM (
  SELECT
    work_external_user_id,
    customer_id,
    card_type_new,
    txn_nbr,
    txn_date,
    user_id_txn,
    demand
  FROM prd_crm.dlabs.store_view_sa_customer
  WHERE user_id_txn IS NOT NULL
    AND is_key_sa = '1'
  GROUP BY 1,2,3,4,5,6,7
) 
GROUP BY 1,2,3;

-- 6. 消费金额表
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_consumption_amount;
CREATE TABLE prd_crm.stage.StoreView_Key_SA_Friend_Reach_consumption_amount AS
SELECT
  txn_date,
  user_id,
  card_type_new,
  SUM(demand) AS consumption_amount
FROM (
  SELECT
    work_external_user_id,
    user_id,
    name,
    txn_date,
    txn_nbr,
    card_type_new,
    demand
  FROM prd_crm.dlabs.store_view_sa_customer 
  WHERE user_id IS NOT NULL
    AND is_key_sa = '1'
  GROUP BY 1,2,3,4,5,6,7
) 
GROUP BY 1,2,3;

-- 7. 金额汇总表
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_amout_summary_sep;
CREATE TABLE prd_crm.stage.StoreView_Key_SA_Friend_Reach_amout_summary_sep AS
SELECT 
  COALESCE(t1.txn_date,t2.txn_date) AS txn_date,
  COALESCE(t1.user_id_txn,t2.user_id) AS user_id,
  COALESCE(t1.card_type_new,t2.card_type_new) AS card_type_new,
  t1.successful_sales_amount,
  t2.consumption_amount
FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_successful_sales_amount t1
FULL OUTER JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_consumption_amount t2
  ON t1.txn_date = t2.txn_date
  AND t1.user_id_txn = t2.user_id
  AND t1.card_type_new = t2.card_type_new
WHERE COALESCE(t1.txn_date,t2.txn_date) IS NOT NULL;

-- 8. 接收者信息表
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_receiver;
CREATE TABLE prd_crm.stage.StoreView_Key_SA_Friend_Reach_receiver
DISTKEY(openid) SORTKEY(openid) AS
SELECT 
  t1.work_external_user_id,
  t2.openid,
  t1.customer_id,
  t1.card_type_new
FROM (
  SELECT 
    work_external_user_id,
    card_type_new,
    customer_id
  FROM prd_crm.dlabs.store_view_sa_customer
  GROUP BY 1,2,3
) t1
LEFT JOIN (
  SELECT 
    openid,
    id,
    customer_id
  FROM prd_c360.landing.weclient_wx_user_corp_external  
  GROUP BY 1,2,3
) t2 ON t1.work_external_user_id = t2.id;

-- 9. 聊天记录基础表
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_chat_base;
CREATE TABLE prd_crm.stage.StoreView_Key_SA_Friend_Reach_chat_base AS
SELECT 
  c.sender_external_userid AS sender,
  c.receiver_external_userid AS receiver,
  CAST(c.msgtime AS TIMESTAMP) AS msgtime,
  DATE(CAST(c.msgtime AS TIMESTAMP)) AS msg_date
FROM prd_crm.cdlcrm.cdl_customer_sa_wecom_chat c
JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_fy_firstday fy 
  ON DATE(CAST(c.msgtime AS TIMESTAMP)) BETWEEN fy.start_timewindow AND DATE(GETDATE() + INTERVAL '8 hours')
WHERE SPLIT_PART(c.msgtime,' ',2) BETWEEN '10:00:00' AND '22:00:00'
  AND c.brand_code = 'Coach'
  AND c.sender_external_userid IN (SELECT user_id FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_sa_table)
  AND (c.sender_external_userid != '' OR c.receiver_external_userid != '')
  AND c.receiver_external_userid NOT IN (SELECT user_id FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_sa_table);

-- 10. 会话标记表
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_session_markers;
CREATE TABLE prd_crm.stage.StoreView_Key_SA_Friend_Reach_session_markers
DISTKEY(sender) SORTKEY(sender, receiver, msg_date) AS
SELECT 
  sender,
  receiver,
  msg_date,
  msgtime,
  CASE 
    WHEN DATEDIFF(minute, LAG(msgtime) OVER (PARTITION BY sender, receiver, msg_date ORDER BY msgtime), msgtime) > 60
    OR LAG(msgtime) OVER (PARTITION BY sender, receiver, msg_date ORDER BY msgtime) IS NULL 
    THEN 1 
    ELSE 0 
  END AS is_new_session
FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_chat_base;

-- 11. 财年日历表
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_fiscal_date;
CREATE TABLE prd_crm.stage.StoreView_Key_SA_Friend_Reach_fiscal_date AS
SELECT 
  d.msg_date,
  w.time_period AS fiscal_week,
  m.time_period AS fiscal_month,
  q.time_period AS fiscal_quarter,
  y.time_period AS fiscal_year
FROM (
  SELECT DISTINCT msg_date 
  FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_chat_base
) d
LEFT JOIN prd_crm.dlabs.dlab_calendar w 
  ON d.msg_date BETWEEN w.start_date AND w.end_date AND w.time_period_code = 'Week'
LEFT JOIN prd_crm.dlabs.dlab_calendar m 
  ON d.msg_date BETWEEN m.start_date AND m.end_date AND m.time_period_code = 'Month'
LEFT JOIN prd_crm.dlabs.dlab_calendar q 
  ON d.msg_date BETWEEN q.start_date AND q.end_date AND q.time_period_code = 'Quarter'
LEFT JOIN prd_crm.dlabs.dlab_calendar y 
  ON d.msg_date BETWEEN y.start_date AND y.end_date AND y.time_period_code = 'Year';

-- 12. 关键指标中间表
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_session_counts;
-- 先预聚合指标再JOIN维度
CREATE TABLE prd_crm.stage.StoreView_Key_SA_Friend_Reach_session_counts AS
WITH pre_agg AS (
  SELECT
    s.msg_date,
    s.sender,
    s.receiver,
    SUM(s.is_new_session) AS total_sessions,
    BOOL_OR(total_sessions >= 2)::INT AS is_quality_session
  FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_session_markers s
  GROUP BY 1,2,3
)
SELECT 
  p.msg_date,
  f.fiscal_week,
  f.fiscal_month,
  f.fiscal_quarter,
  f.fiscal_year,
  p.sender AS user_id,
  sa.name,
  sa.local_name,
  sa.store_nbr,
  sa.counter_type_code,
  sa.region,
  sa.region_datail,
  p.receiver,
  r.work_external_user_id,
  r.card_type_new,
  p.total_sessions AS is_session,
  p.is_quality_session
FROM pre_agg p
JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_sa_table sa 
  ON p.sender = sa.user_id
JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_receiver r
  ON p.receiver = r.openid
JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_fiscal_date f 
  ON p.msg_date = f.msg_date;

-- 13. 当前周期表
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_current_period;
CREATE TABLE prd_crm.stage.StoreView_Key_SA_Friend_Reach_current_period AS
SELECT 
  fiscal_year AS current_year,
  MAX(fiscal_quarter) AS current_quarter,
  MAX(fiscal_month) AS current_month,
  MAX(fiscal_week) AS current_week,
  MAX(msg_date) AS current_date
FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_session_counts
GROUP BY fiscal_year;

-- 14. 最终结果表
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach;
CREATE TABLE prd_crm.stage.StoreView_Key_SA_Friend_Reach AS
WITH summary AS (
  -- DTD类型2-4
  SELECT 
    t1.user_id AS sa_code,
    t1.name AS sa_name,
    t1.store_nbr AS store_code,
    t1.local_name AS store_name,
    t1.fiscal_year AS year,
    'DTD' AS time_period,
    REPLACE(t1.msg_date,'-','') AS ordinal_number,
    CASE 
      WHEN t1.card_type_new IN ('DIAMOND,','GOLD','Regular','SILVER') THEN 2
      WHEN t1.card_type_new IN ('Non-Member') THEN 4
      ELSE 3 
    END AS type,
    SUM(t3.people) AS people,
    SUM(t1.is_session) AS commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_session >= 1 THEN t1.receiver END) AS commnunation_people_count,
    SUM(t1.is_quality_session) AS quality_commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_quality_session = 1 THEN t1.receiver END) AS quality_commnunation_people_count,
    SUM(t2.successful_sales_amount) AS successful_sales_amount,
    SUM(t2.consumption_amount) AS consumption_amount
  FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_session_counts t1
  LEFT JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_amout_summary_sep t2
    ON t1.msg_date = t2.txn_date
    AND t1.user_id = t2.user_id
    AND t1.card_type_new = t2.card_type_new
  LEFT JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_add_total t3
    ON t3.add_date <= t1.msg_date
    AND t1.user_id = t3.user_id
    AND t1.card_type_new = t3.card_type_new
  GROUP BY 1,2,3,4,5,6,7,8
  
  UNION ALL
  
  -- DTD类型1
  SELECT 
    t1.user_id AS sa_code,
    t1.name AS sa_name,
    t1.store_nbr AS store_code,
    t1.local_name AS store_name,
    t1.fiscal_year AS year,
    'DTD' AS time_period,
    REPLACE(t1.msg_date,'-','') AS ordinal_number,
    1 AS type,
    SUM(t3.people) AS people,
    SUM(t1.is_session) AS commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_session >= 1 THEN t1.receiver END) AS commnunation_people_count,
    SUM(t1.is_quality_session) AS quality_commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_quality_session = 1 THEN t1.receiver END) AS quality_commnunation_people_count,
    SUM(t2.successful_sales_amount) AS successful_sales_amount,
    SUM(t2.consumption_amount) AS consumption_amount
  FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_session_counts t1
  LEFT JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_amout_summary_sep t2
    ON t1.msg_date = t2.txn_date
    AND t1.user_id = t2.user_id
    AND t1.card_type_new = t2.card_type_new
  LEFT JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_add_total t3
    ON t3.add_date <= t1.msg_date
    AND t1.user_id = t3.user_id
    AND t1.card_type_new = t3.card_type_new
  GROUP BY 1,2,3,4,5,6,7,8
  
  UNION ALL
  
  -- WTD类型2-4
  SELECT 
    t1.user_id AS sa_code,
    t1.name AS sa_name,
    t1.store_nbr AS store_code,
    t1.local_name AS store_name,
    t1.fiscal_year AS year,
    'WTD' AS time_period,
    SUBSTRING(t1.fiscal_week,5,2) AS ordinal_number,
    CASE 
      WHEN t1.card_type_new IN ('DIAMOND,','GOLD','Regular','SILVER') THEN 2
      WHEN t1.card_type_new IN ('Non-Member') THEN 4
      ELSE 3 
    END AS type,
    SUM(t3.people) AS people,
    SUM(t1.is_session) AS commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_session >= 1 THEN t1.receiver END) AS commnunation_people_count,
    SUM(t1.is_quality_session) AS quality_commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_quality_session = 1 THEN t1.receiver END) AS quality_commnunation_people_count,
    SUM(t2.successful_sales_amount) AS successful_sales_amount,
    SUM(t2.consumption_amount) AS consumption_amount
  FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_session_counts t1
  JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_current_period cp
    ON t1.fiscal_week <= cp.current_week
  LEFT JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_amout_summary_sep t2
    ON t1.msg_date = t2.txn_date
    AND t1.user_id = t2.user_id
    AND t1.card_type_new = t2.card_type_new
  LEFT JOIN (
    SELECT * 
    FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_add_total
    JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_current_period
      ON add_date <= current_date
  ) t3
    ON t1.user_id = t3.user_id
    AND t1.card_type_new = t3.card_type_new
  GROUP BY 1,2,3,4,5,6,7,8
  
  UNION ALL
  
  -- WTD类型1
  SELECT 
    t1.user_id AS sa_code,
    t1.name AS sa_name,
    t1.store_nbr AS store_code,
    t1.local_name AS store_name,
    t1.fiscal_year AS year,
    'WTD' AS time_period,
    SUBSTRING(t1.fiscal_week,5,2) AS ordinal_number,
    1 AS type,
    SUM(t3.people) AS people,
    SUM(t1.is_session) AS commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_session >= 1 THEN t1.receiver END) AS commnunation_people_count,
    SUM(t1.is_quality_session) AS quality_commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_quality_session = 1 THEN t1.receiver END) AS quality_commnunation_people_count,
    SUM(t2.successful_sales_amount) AS successful_sales_amount,
    SUM(t2.consumption_amount) AS consumption_amount
  FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_session_counts t1
  JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_current_period cp
    ON t1.fiscal_week <= cp.current_week
  LEFT JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_amout_summary_sep t2
    ON t1.msg_date = t2.txn_date
    AND t1.user_id = t2.user_id
    AND t1.card_type_new = t2.card_type_new
  LEFT JOIN (
    SELECT * 
    FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_add_total
    JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_current_period
      ON add_date <= current_date
  ) t3
    ON t1.user_id = t3.user_id
    AND t1.card_type_new = t3.card_type_new
  GROUP BY 1,2,3,4,5,6,7,8
  
  UNION ALL
  
  -- MTD类型2-4
  SELECT 
    t1.user_id AS sa_code,
    t1.name AS sa_name,
    t1.store_nbr AS store_code,
    t1.local_name AS store_name,
    t1.fiscal_year AS year,
    'MTD' AS time_period,
    SUBSTRING(t1.fiscal_month,5,2) AS ordinal_number,
    CASE 
      WHEN t1.card_type_new IN ('DIAMOND,','GOLD','Regular','SILVER') THEN 2
      WHEN t1.card_type_new IN ('Non-Member') THEN 4
      ELSE 3 
    END AS type,
    SUM(t3.people) AS people,
    SUM(t1.is_session) AS commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_session >= 1 THEN t1.receiver END) AS commnunation_people_count,
    SUM(t1.is_quality_session) AS quality_commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_quality_session = 1 THEN t1.receiver END) AS quality_commnunation_people_count,
    SUM(t2.successful_sales_amount) AS successful_sales_amount,
    SUM(t2.consumption_amount) AS consumption_amount
  FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_session_counts t1
  JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_current_period cp
    ON t1.fiscal_month <= cp.current_month
  LEFT JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_amout_summary_sep t2
    ON t1.msg_date = t2.txn_date
    AND t1.user_id = t2.user_id
    AND t1.card_type_new = t2.card_type_new
  LEFT JOIN (
    SELECT * 
    FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_add_total
    JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_current_period
      ON add_date <= current_date
  ) t3
    ON t1.user_id = t3.user_id
    AND t1.card_type_new = t3.card_type_new
  GROUP BY 1,2,3,4,5,6,7,8
  
  UNION ALL
  
  -- MTD类型1
  SELECT 
    t1.user_id AS sa_code,
    t1.name AS sa_name,
    t1.store_nbr AS store_code,
    t1.local_name AS store_name,
    t1.fiscal_year AS year,
    'MTD' AS time_period,
    SUBSTRING(t1.fiscal_month,5,2) AS ordinal_number,
    1 AS type,
    SUM(t3.people) AS people,
    SUM(t1.is_session) AS commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_session >= 1 THEN t1.receiver END) AS commnunation_people_count,
    SUM(t1.is_quality_session) AS quality_commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_quality_session = 1 THEN t1.receiver END) AS quality_commnunation_people_count,
    SUM(t2.successful_sales_amount) AS successful_sales_amount,
    SUM(t2.consumption_amount) AS consumption_amount
  FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_session_counts t1
  JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_current_period cp
    ON t1.fiscal_month <= cp.current_month
  LEFT JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_amout_summary_sep t2
    ON t1.msg_date = t2.txn_date
    AND t1.user_id = t2.user_id
    AND t1.card_type_new = t2.card_type_new
  LEFT JOIN (
    SELECT * 
    FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_add_total
    JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_current_period
      ON add_date <= current_date
  ) t3
    ON t1.user_id = t3.user_id
    AND t1.card_type_new = t3.card_type_new
  GROUP BY 1,2,3,4,5,6,7,8
  
  UNION ALL
  
  -- QTD类型2-4
  SELECT 
    t1.user_id AS sa_code,
    t1.name AS sa_name,
    t1.store_nbr AS store_code,
    t1.local_name AS store_name,
    t1.fiscal_year AS year,
    'QTD' AS time_period,
    SUBSTRING(t1.fiscal_quarter,5,2) AS ordinal_number,
    CASE 
      WHEN t1.card_type_new IN ('DIAMOND,','GOLD','Regular','SILVER') THEN 2
      WHEN t1.card_type_new IN ('Non-Member') THEN 4
      ELSE 3 
    END AS type,
    SUM(t3.people) AS people,
    SUM(t1.is_session) AS commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_session >= 1 THEN t1.receiver END) AS commnunation_people_count,
    SUM(t1.is_quality_session) AS quality_commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_quality_session = 1 THEN t1.receiver END) AS quality_commnunation_people_count,
    SUM(t2.successful_sales_amount) AS successful_sales_amount,
    SUM(t2.consumption_amount) AS consumption_amount
  FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_session_counts t1
  JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_current_period cp
    ON t1.fiscal_quarter <= cp.current_quarter
  LEFT JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_amout_summary_sep t2
    ON t1.msg_date = t2.txn_date
    AND t1.user_id = t2.user_id
    AND t1.card_type_new = t2.card_type_new
  LEFT JOIN (
    SELECT * 
    FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_add_total
    JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_current_period
      ON add_date <= current_date
  ) t3
    ON t1.user_id = t3.user_id
    AND t1.card_type_new = t3.card_type_new
  GROUP BY 1,2,3,4,5,6,7,8
  
  UNION ALL
  
  -- QTD类型1
  SELECT 
    t1.user_id AS sa_code,
    t1.name AS sa_name,
    t1.store_nbr AS store_code,
    t1.local_name AS store_name,
    t1.fiscal_year AS year,
    'QTD' AS time_period,
    SUBSTRING(t1.fiscal_quarter,5,2) AS ordinal_number,
    1 AS type,
    SUM(t3.people) AS people,
    SUM(t1.is_session) AS commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_session >= 1 THEN t1.receiver END) AS commnunation_people_count,
    SUM(t1.is_quality_session) AS quality_commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_quality_session = 1 THEN t1.receiver END) AS quality_commnunation_people_count,
    SUM(t2.successful_sales_amount) AS successful_sales_amount,
    SUM(t2.consumption_amount) AS consumption_amount
  FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_session_counts t1
  JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_current_period cp
    ON t1.fiscal_quarter <= cp.current_quarter
  LEFT JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_amout_summary_sep t2
    ON t1.msg_date = t2.txn_date
    AND t1.user_id = t2.user_id
    AND t1.card_type_new = t2.card_type_new
  LEFT JOIN (
    SELECT * 
    FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_add_total
    JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_current_period
      ON add_date <= current_date
  ) t3
    ON t1.user_id = t3.user_id
    AND t1.card_type_new = t3.card_type_new
  GROUP BY 1,2,3,4,5,6,7,8
  
  UNION ALL
  
  -- YTD类型2-4
  SELECT 
    t1.user_id AS sa_code,
    t1.name AS sa_name,
    t1.store_nbr AS store_code,
    t1.local_name AS store_name,
    t1.fiscal_year AS year,
    'YTD' AS time_period,
    CAST(t1.fiscal_year AS VARCHAR) AS ordinal_number,
    CASE 
      WHEN t1.card_type_new IN ('DIAMOND,','GOLD','Regular','SILVER') THEN 2
      WHEN t1.card_type_new IN ('Non-Member') THEN 4
      ELSE 3 
    END AS type,
    SUM(t3.people) AS people,
    SUM(t1.is_session) AS commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_session >= 1 THEN t1.receiver END) AS commnunation_people_count,
    SUM(t1.is_quality_session) AS quality_commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_quality_session = 1 THEN t1.receiver END) AS quality_commnunation_people_count,
    SUM(t2.successful_sales_amount) AS successful_sales_amount,
    SUM(t2.consumption_amount) AS consumption_amount
  FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_session_counts t1
  JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_current_period cp
    ON t1.fiscal_year = cp.current_year
  LEFT JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_amout_summary_sep t2
    ON t1.msg_date = t2.txn_date
    AND t1.user_id = t2.user_id
    AND t1.card_type_new = t2.card_type_new
  LEFT JOIN (
    SELECT * 
    FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_add_total
    JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_current_period
      ON add_date <= current_date
  ) t3
    ON t1.user_id = t3.user_id
    AND t1.card_type_new = t3.card_type_new
  GROUP BY 1,2,3,4,5,6,7,8
  
  UNION ALL
  
  -- YTD类型1
  SELECT 
    t1.user_id AS sa_code,
    t1.name AS sa_name,
    t1.store_nbr AS store_code,
    t1.local_name AS store_name,
    t1.fiscal_year AS year,
    'YTD' AS time_period,
    CAST(t1.fiscal_year AS VARCHAR) AS ordinal_number,
    1 AS type,
    SUM(t3.people) AS people,
    SUM(t1.is_session) AS commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_session >= 1 THEN t1.receiver END) AS commnunation_people_count,
    SUM(t1.is_quality_session) AS quality_commnunation_count,
    COUNT(DISTINCT CASE WHEN t1.is_quality_session = 1 THEN t1.receiver END) AS quality_commnunation_people_count,
    SUM(t2.successful_sales_amount) AS successful_sales_amount,
    SUM(t2.consumption_amount) AS consumption_amount
  FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_session_counts t1
  JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_current_period cp
    ON t1.fiscal_year = cp.current_year
  LEFT JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_amout_summary_sep t2
    ON t1.msg_date = t2.txn_date
    AND t1.user_id = t2.user_id
    AND t1.card_type_new = t2.card_type_new
  LEFT JOIN (
    SELECT * 
    FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_add_total
    JOIN prd_crm.stage.StoreView_Key_SA_Friend_Reach_current_period
      ON add_date <= current_date
  ) t3
    ON t1.user_id = t3.user_id
    AND t1.card_type_new = t3.card_type_new
  GROUP BY 1,2,3,4,5,6,7,8
)
SELECT
  (SELECT MAX(add_date) FROM prd_crm.stage.StoreView_Key_SA_Friend_Reach_add_total) AS data_date,
  t1.*
FROM summary t1;

-- 15. 清理中间表
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_fy_firstday;
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_sa_table;
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_max_year;
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_add_total;
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_successful_sales_amount;
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_consumption_amount;
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_amout_summary_sep;
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_receiver;
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_chat_base;
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_session_markers;
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_fiscal_date;
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_session_counts;
DROP TABLE IF EXISTS prd_crm.stage.StoreView_Key_SA_Friend_Reach_current_period;
