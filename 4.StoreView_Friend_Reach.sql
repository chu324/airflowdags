--4. StoreView_Friend_Reach
drop table  IF EXISTS prd_crm.stage.StoreView_Friend_Reach;
create table prd_crm.stage.StoreView_Friend_Reach as 
with sa_table as (
    select DISTINCT user_id,name,local_name,store_nbr,counter_type_code,region,region_datail
    from prd_crm.dlabs.store_view_sa_customer
    --Status: 0-created;1- actived; 2 Banned; 4 - Not actived; 5 Exit
    where sa_status in ('0','1','4')
      and local_name not in ('TW EDA (Temp Store)_OCF23')
),
     max_year as (select min(add_date) as fy_start_date,
                         max(cast(20 || substring(fisc_year, 3, 2) as int)) as year
                  from prd_crm.dlabs.store_view_sa_customer
                  where cast(20 || substring(fisc_year, 3, 2) as int) in (
                      (select cast(20 || substring(max(fisc_year), 3, 2) as int) as year_max
                       from prd_crm.dlabs.store_view_sa_customer))),
     -- 1. calculate the sa sales with adding info list, successful sale, consumotion amount
     add_total as (

         select add_date,user_id,card_type_new,count(distinct work_external_user_id) as people
         from (
                  select
                      work_external_user_id,user_id,name,add_date,card_type_new
                  from prd_crm.dlabs.store_view_sa_customer where user_id is not null and add_rank =1
                                                              and add_date>'1970-01-01'
                  group by  1,2,3,4,5)
         group by 1,2,3
     ),
     successful_sales_amount as (
         select
             txn_date,user_id_txn,card_type_new,
             sum(demand) as successful_sales_amount
         from (
                  select
                      work_external_user_id,customer_id,card_type_new,txn_nbr,txn_date,user_id_txn,demand
                  from prd_crm.dlabs.store_view_sa_customer
                  where user_id_txn is not null
                  group by  1,2,3,4,5,6,7) group by 1,2,3
     ),
     consumption_amount as (
         select
             txn_date,user_id,card_type_new,
             sum(demand) as consumption_amount
         from (
                  select
                      work_external_user_id,user_id,name,txn_date,txn_nbr,card_type_new,demand
                  from prd_crm.dlabs.store_view_sa_customer where user_id is not null
                  group by  1,2,3,4,5,6,7) group by 1,2,3
     ),
     amout_summary_sep as (
         select * from (
                           select
                               coalesce(t1.txn_date,t2.txn_date) as txn_date,
                               coalesce(t1.user_id_txn,t2.user_id) as user_id,
                               coalesce(t1.card_type_new,t2.card_type_new) as card_type_new,
                               t1.successful_sales_amount as successful_sales_amount,
                               t2.consumption_amount as consumption_amount
                           from successful_sales_amount t1
                                    full outer join  consumption_amount t2
                                                     on t1.txn_date = t2.txn_date
                                                         and t1.user_id_txn = t2.user_id
                                                         and t1.card_type_new= t2.card_type_new

                       )
         where txn_date is not null
     )
        ,
     -- 2. Reciver Id and info list
     receiver as (

         select t1.work_external_user_id,t2.openid,t1.customer_id,t1.card_type_new
         from (  select work_external_user_id,card_type_new,customer_id
                 from prd_crm.dlabs.store_view_sa_customer
                 group by 1,2,3) t1
                  left join (
             select openid,id,customer_id
             from prd_c360.landing.weclient_wx_user_corp_external  group by 1,2,3) t2
                            on t1.work_external_user_id = t2.id
     ),
     -- 3. Filter the msg with customer and between 10:00-22:00
     filter_msg as(
         select sender_external_userid as sender,receiver_external_userid as receiver ,
                cast(msgtime as timestamp) as msgtime,
                date(cast(msgtime as timestamp)) as msg_date
         from prd_crm.cdlcrm.cdl_customer_sa_wecom_chat,max_year
         where split_part(msgtime,' ',2) between '10:00:00' and '22:00:00'
           and  brand_code in ('Coach')
           and sender_external_userid in ( select user_id from sa_table group by 1)
           and (sender_external_userid  not in ('') or receiver_external_userid  not in (''))
           and receiver_external_userid not in (select user_id from sa_table group by 1)
           and date(cast(msgtime as timestamp)) >=fy_start_date
         group by 1,2,3,4
     ),
     -- 4. >60 considered as 1 session or only 1 message without any feedback is also the 1 session
     session_markers as (
         select sender,receiver,msg_date,msgtime,
                case when datediff(minute,lag(msgtime) over (partition by sender,receiver,msg_date order by msgtime),
                                   msgtime)>60
                    or lag(msgtime) over (partition by sender,receiver,msg_date order by msgtime) is null
                         then 1 else 0 end as is_new_session
         from filter_msg
     ),
     -- 5. find fiscal date of the msg_date
     fiscal_date as (
         select t1.msg_date,
                t3.time_period as fiscal_week,
                t4.time_period as fiscal_month,
                t5.time_period as fiscal_quarter,
                t6.time_period as fiscal_year
         from
             (select msg_date from session_markers group by 1) t1
                 left join
             (select * from prd_crm.dlabs.dlab_calendar where time_period_code in ('Week') ) t3
             on t1.msg_date between t3.start_date and t3.end_date
                 left join  (select * from
                 prd_crm.dlabs.dlab_calendar where time_period_code in ('Month')
             ) t4
                            on t1.msg_date between t4.start_date and t4.end_date
                 left join  (select * from
                 prd_crm.dlabs.dlab_calendar where time_period_code in ('Quarter')
             ) t5
                            on t1.msg_date between t5.start_date and t5.end_date
                 left join  (select * from
                 prd_crm.dlabs.dlab_calendar where time_period_code in ('Year')
             ) t6
                            on t1.msg_date between t6.start_date and t6.end_date
     ) ,
     session_counts as (
         select t1.msg_date,fiscal_week,fiscal_month,fiscal_quarter,fiscal_year,
                sender,name,local_name,store_nbr,counter_type_code,region,region_datail,
                receiver,t3.work_external_user_id,t3.card_type_new,
                sum(is_new_session) as is_session,
                case when sum(is_new_session)>=2 then 1 else 0 end as is_quality_session
         from session_markers t1
                  left join (
             select user_id,name,local_name,store_nbr,counter_type_code,region,region_datail
             from sa_table
         ) t2
                            on t1.sender = t2.user_id
                  left join receiver t3
                            on t1.receiver = t3.openid
                  left join fiscal_date t4
                            on t1.msg_date = t4.msg_date
         group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
     ),
     current_period as (
         select
             fiscal_year as current_year,
             max(fiscal_quarter) as current_quarter,
             max(fiscal_month) as current_month,
             max(fiscal_week) as current_week,
         max(msg_date) as current_date
         from session_counts
         group by 1
     ),
     -- 4. 门店
     summary_store4 as (
         -- 1. 'DTD'
         select  4 as organize_type,
                 store_nbr as organize,
                 local_name as store_name,
                 fiscal_year as year,
                'DTD' as time_period,
                replace(msg_date,'-','') as ordinal_number,
                case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                     when t1.card_type_new in ('Non-Member') then 4
                     else 3 end as type,
                sum(people) as people,
                sum(is_session) as commnunation_count,
                count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                sum(is_quality_session) as quality_commnunation_count,
                count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                sum(t2.successful_sales_amount) as successful_sales_amount,
                sum(t2.consumption_amount) as consumption_amount
         from session_counts t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  add_total t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  4 as organize_type,
                 store_nbr as organize,
                 local_name as store_name,
                 fiscal_year as year,
                 'DTD' as time_period,
                 replace(msg_date,'-','') as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from session_counts t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  add_total t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
             union all
                  -- 2. 'WTD'
             select  4 as organize_type,
                 store_nbr as organize,
                 local_name as store_name,
                 fiscal_year as year,
                'WTD' as time_period,
                 substring(fiscal_week,5,2) as ordinal_number,
         case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
         when t1.card_type_new in ('Non-Member') then 4
         else 3 end as type,
         sum(people) as people,
         sum(is_session) as commnunation_count,
         count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
         sum(is_quality_session) as quality_commnunation_count,
         count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
         sum(t2.successful_sales_amount) as successful_sales_amount,
         sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_week=current_week ) t1
             left join  amout_summary_sep t2
         on t1.msg_date = t2.txn_date
             and t1.sender = t2.user_id
             and t1.card_type_new = t2.card_type_new
             left join  (select * from add_total,current_period where add_date <=current_date)  t3
             on  t1.sender = t3.user_id
             and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  4 as organize_type,
             store_nbr as organize,
             local_name as store_name,
             fiscal_year as year,
                 'WTD' as time_period,
                 substring(fiscal_week,5,2) as ordinal_number,
         1 as type,
         sum(people) as people,
         sum(is_session) as commnunation_count,
         count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
         sum(is_quality_session) as quality_commnunation_count,
         count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
         sum(t2.successful_sales_amount) as successful_sales_amount,
         sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_week=current_week ) t1
             left join  amout_summary_sep t2
         on t1.msg_date = t2.txn_date
             and t1.sender = t2.user_id
             and t1.card_type_new = t2.card_type_new
            left join  (select * from add_total,current_period where add_date <=current_date)  t3
             on t3.add_date <=t1.msg_date
             and t1.sender = t3.user_id
             and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         -- 3. 'MTD'
         select  4 as organize_type,
                 store_nbr as organize,
                 local_name as store_name,
                 fiscal_year as year,
                 'MTD' as time_period,
                 substring(fiscal_month,5,2) as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_month=current_month ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on  t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  4 as organize_type,
                 store_nbr as organize,
                 local_name as store_name,
                 fiscal_year as year,
                 'MTD' as time_period,
                 substring(fiscal_month,5,2) as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_month=current_month) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         -- 4. 'QTD'
         select  4 as organize_type,
                 store_nbr as organize,
                 local_name as store_name,
                 fiscal_year as year,
                 'QTD' as time_period,
                 substring(fiscal_quarter,5,2) as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_quarter=current_quarter ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on  t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  4 as organize_type,
                 store_nbr as organize,
                 local_name as store_name,
                 fiscal_year as year,
                 'QTD' as time_period,
                 substring(fiscal_quarter,5,2) as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_quarter=current_quarter) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         -- 5. 'YTD'
         select  4 as organize_type,
                 store_nbr as organize,
                 local_name as store_name,
                 fiscal_year as year,
                 'YTD' as time_period,
                 cast(fiscal_year as varchar) as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_year=current_year ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on  t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  4 as organize_type,
                 store_nbr as organize,
                 local_name as store_name,
                 fiscal_year as year,
                 'YTD' as time_period,
                 cast(fiscal_year as varchar)  as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_year=current_year) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
     ),
    --  3. 分区
     summary_store3 as (
         -- 1. 'DTD'
         select  3 as organize_type,
                 region_datail as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'DTD' as time_period,
                 replace(msg_date,'-','') as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from session_counts t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  add_total t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  3 as organize_type,
                 region_datail as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'DTD' as time_period,
                 replace(msg_date,'-','') as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from session_counts t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  add_total t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         -- 2. 'WTD'
         select  3 as organize_type,
                 region_datail as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'WTD' as time_period,
                 substring(fiscal_week,5,2) as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_week=current_week ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on  t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  3 as organize_type,
                 region_datail as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'WTD' as time_period,
                 substring(fiscal_week,5,2) as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_week=current_week ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         -- 3. 'MTD'
         select  3 as organize_type,
                 region_datail as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'MTD' as time_period,
                 substring(fiscal_month,5,2) as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_month=current_month ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on  t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  3 as organize_type,
                 region_datail as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'MTD' as time_period,
                 substring(fiscal_month,5,2) as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_month=current_month) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         -- 4. 'QTD'
         select  3 as organize_type,
                 region_datail as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'QTD' as time_period,
                 substring(fiscal_quarter,5,2) as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_quarter=current_quarter ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on  t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  3 as organize_type,
                 region_datail as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'QTD' as time_period,
                 substring(fiscal_quarter,5,2) as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_quarter=current_quarter) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         -- 5. 'YTD'
         select  3 as organize_type,
                 region_datail as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'YTD' as time_period,
                 cast(fiscal_year as varchar) as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_year=current_year ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on  t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  3 as organize_type,
                 region_datail as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'YTD' as time_period,
                 cast(fiscal_year as varchar)  as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_year=current_year) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
     ),
     --  2. 大区
     summary_store2 as (
         -- 1. 'DTD'
         select  2 as organize_type,
                 region as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'DTD' as time_period,
                 replace(msg_date,'-','') as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from session_counts t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  add_total t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  2 as organize_type,
                 region as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'DTD' as time_period,
                 replace(msg_date,'-','') as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from session_counts t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  add_total t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         -- 2. 'WTD'
         select  2 as organize_type,
                 region as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'WTD' as time_period,
                 substring(fiscal_week,5,2) as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_week=current_week ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on  t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  2 as organize_type,
                 region as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'WTD' as time_period,
                 substring(fiscal_week,5,2) as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_week=current_week ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         -- 3. 'MTD'
         select  2 as organize_type,
                 region as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'MTD' as time_period,
                 substring(fiscal_month,5,2) as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_month=current_month ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on  t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  2 as organize_type,
                 region as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'MTD' as time_period,
                 substring(fiscal_month,5,2) as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_month=current_month) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         -- 4. 'QTD'
         select  2 as organize_type,
                 region as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'QTD' as time_period,
                 substring(fiscal_quarter,5,2) as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_quarter=current_quarter ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on  t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select 2 as organize_type,
                region as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'QTD' as time_period,
                 substring(fiscal_quarter,5,2) as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_quarter=current_quarter) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         -- 5. 'YTD'
         select  2 as organize_type,
                 region as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'YTD' as time_period,
                 cast(fiscal_year as varchar) as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_year=current_year ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on  t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  2 as organize_type,
                 region as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'YTD' as time_period,
                 cast(fiscal_year as varchar)  as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_year=current_year) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
     ),
     --  1. 渠道
     summary_store1 as (
         -- 1. 'DTD'
         select  1 as organize_type,
                 case when counter_type_code in ('Factory') then 'PRC Outlet Total'
                      when counter_type_code in ('Regular') then 'PRC Retail Total'
                      else counter_type_code end as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'DTD' as time_period,
                 replace(msg_date,'-','') as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from session_counts t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  add_total t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  1 as organize_type,
                 case when counter_type_code in ('Factory') then 'PRC Outlet Total'
                      when counter_type_code in ('Regular') then 'PRC Retail Total'
                      else counter_type_code end as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'DTD' as time_period,
                 replace(msg_date,'-','') as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from session_counts t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  add_total t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         -- 2. 'WTD'
         select  1 as organize_type,
                 case when counter_type_code in ('Factory') then 'PRC Outlet Total'
                      when counter_type_code in ('Regular') then 'PRC Retail Total'
                      else counter_type_code end as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'WTD' as time_period,
                 substring(fiscal_week,5,2) as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_week=current_week ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on  t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  1 as organize_type,
                 case when counter_type_code in ('Factory') then 'PRC Outlet Total'
                      when counter_type_code in ('Regular') then 'PRC Retail Total'
                      else counter_type_code end as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'WTD' as time_period,
                 substring(fiscal_week,5,2) as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_week=current_week ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         -- 3. 'MTD'
         select  1 as organize_type,
                 case when counter_type_code in ('Factory') then 'PRC Outlet Total'
                      when counter_type_code in ('Regular') then 'PRC Retail Total'
                      else counter_type_code end as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'MTD' as time_period,
                 substring(fiscal_month,5,2) as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_month=current_month ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on  t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  1 as organize_type,
                 case when counter_type_code in ('Factory') then 'PRC Outlet Total'
                      when counter_type_code in ('Regular') then 'PRC Retail Total'
                      else counter_type_code end as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'MTD' as time_period,
                 substring(fiscal_month,5,2) as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_month=current_month) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         -- 4. 'QTD'
         select  1 as organize_type,
                 case when counter_type_code in ('Factory') then 'PRC Outlet Total'
                      when counter_type_code in ('Regular') then 'PRC Retail Total'
                      else counter_type_code end as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'QTD' as time_period,
                 substring(fiscal_quarter,5,2) as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_quarter=current_quarter ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on  t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select 1 as organize_type,
                case when counter_type_code in ('Factory') then 'PRC Outlet Total'
                     when counter_type_code in ('Regular') then 'PRC Retail Total'
                     else counter_type_code end as organize,
                '' as store_name,
                fiscal_year as year,
                'QTD' as time_period,
                substring(fiscal_quarter,5,2) as ordinal_number,
                1 as type,
                sum(people) as people,
                sum(is_session) as commnunation_count,
                count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                sum(is_quality_session) as quality_commnunation_count,
                count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                sum(t2.successful_sales_amount) as successful_sales_amount,
                sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_quarter=current_quarter) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         -- 5. 'YTD'
         select  1 as organize_type,
                 case when counter_type_code in ('Factory') then 'PRC Outlet Total'
                      when counter_type_code in ('Regular') then 'PRC Retail Total'
                      else counter_type_code end as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'YTD' as time_period,
                 cast(fiscal_year as varchar) as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_year=current_year ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on  t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  1 as organize_type,
                 case when counter_type_code in ('Factory') then 'PRC Outlet Total'
                      when counter_type_code in ('Regular') then 'PRC Retail Total'
                      else counter_type_code end as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'YTD' as time_period,
                 cast(fiscal_year as varchar)  as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_year=current_year) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
     ),
     --  0. Total
     summary_store0 as (
         -- 1. 'DTD'
         select  0 as organize_type,
                 '' as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'DTD' as time_period,
                 replace(msg_date,'-','') as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from session_counts t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  add_total t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  0 as organize_type,
                 '' as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'DTD' as time_period,
                 replace(msg_date,'-','') as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from session_counts t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  add_total t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         -- 2. 'WTD'
         select  0 as organize_type,
                 '' as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'WTD' as time_period,
                 substring(fiscal_week,5,2) as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_week=current_week ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on  t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  0 as organize_type,
                 '' as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'WTD' as time_period,
                 substring(fiscal_week,5,2) as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_week=current_week ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         -- 3. 'MTD'
         select  0 as organize_type,
                 '' as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'MTD' as time_period,
                 substring(fiscal_month,5,2) as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_month=current_month ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on  t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  0 as organize_type,
                 '' as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'MTD' as time_period,
                 substring(fiscal_month,5,2) as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_month=current_month) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         -- 4. 'QTD'
         select  0 as organize_type,
                 '' as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'QTD' as time_period,
                 substring(fiscal_quarter,5,2) as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_quarter=current_quarter ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on  t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select 0 as organize_type,
                '' as organize,
                '' as store_name,
                fiscal_year as year,
                'QTD' as time_period,
                substring(fiscal_quarter,5,2) as ordinal_number,
                1 as type,
                sum(people) as people,
                sum(is_session) as commnunation_count,
                count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                sum(is_quality_session) as quality_commnunation_count,
                count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                sum(t2.successful_sales_amount) as successful_sales_amount,
                sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_quarter=current_quarter) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         -- 5. 'YTD'
         select  0 as organize_type,
                 '' as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'YTD' as time_period,
                 cast(fiscal_year as varchar) as ordinal_number,
                 case when t1.card_type_new in ('DIAMOND,','GOLD','Regular','SILVER') then 2
                      when t1.card_type_new in ('Non-Member') then 4
                      else 3 end as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_year=current_year ) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on  t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
         union all
         select  0 as organize_type,
                 '' as organize,
                 '' as store_name,
                 fiscal_year as year,
                 'YTD' as time_period,
                 cast(fiscal_year as varchar)  as ordinal_number,
                 1 as type,
                 sum(people) as people,
                 sum(is_session) as commnunation_count,
                 count(distinct case when is_session>= 1 then receiver end ) as commnunation_people_count,
                 sum(is_quality_session) as quality_commnunation_count,
                 count(distinct case when is_quality_session= 1 then receiver end ) as quality_commnunation_people_count,
                 sum(t2.successful_sales_amount) as successful_sales_amount,
                 sum(t2.consumption_amount) as consumption_amount
         from (select * from session_counts,current_period where fiscal_year=current_year) t1
                  left join  amout_summary_sep t2
                             on t1.msg_date = t2.txn_date
                                 and t1.sender = t2.user_id
                                 and t1.card_type_new = t2.card_type_new
                  left join  (select * from add_total,current_period where add_date <=current_date)  t3
                             on t3.add_date <=t1.msg_date
                                 and t1.sender = t3.user_id
                                 and t1.card_type_new = t3.card_type_new
         group by 1,2,3,4,5,6,7
     ),
     summary_store as(
         select * from summary_store0
                  union all
         select * from summary_store1
         union all
         select * from summary_store2
         union all
         select * from summary_store3
         union all
         select * from summary_store4

     )
select
    (select max(add_date) from add_total) as data_date,
    t1.*
from
             summary_store t1;
