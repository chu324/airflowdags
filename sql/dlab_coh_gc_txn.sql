/*
DLABS Transaction Tables
Brand: Coach
Region: CA/GC
Author: Victor Le
Version: REDSHIFT V1
Date: 8/29/2018
Update: Updated on 12/9/20 to include cust_first_pur_dt_txns_flag
Update2: Updated on 8/2/21 to increase length of txn_item_line_nbr from 20 to 50
Update3: Updated on 11/1/22 to add additional catch for sku before turning product_id to -9999 ({env}_crm.stage.tmp_coh_gc_sku_lkp)
Inputs: 
	CDL:
	{env}_crm.cdlcrm.cdl_mstr_product_sku
	{env}_crm.cdlcrm.cdl_mstr_product
	{env}_crm.cdlcrm.cdl_customer
	{env}_crm.cdlcrm.cdl_customer_type_history
	{env}_crm.cdlcrm.cdl_mstr_cust_ref_customer_type
	{env}_crm.cdlcrm.cdl_mstr_addr_ref_city
	{env}_crm.cdlcrm.cdl_mstr_addr_ref_province
	{env}_crm.cdlcrm.cdl_mstr_addr_ref_country
	{env}_crm.cdlcrm.cdl_transaction_hdr
	{env}_crm.cdlcrm.cdl_transaction_dtl
	{env}_crm.cdlcrm.cdl_mstr_counter
	{env}_crm.cdlcrm.cdl_mstr_counter_category
	{env}_crm.cdlcrm.cdl_mstr_calendar
	{env}_crm.cdlcrm.cdl_mstr_location
	{env}_crm.cdlcrm.hdw_lkp_comp_ind_mst
	{env}_crm.cdlcrm.lkp_residence_country
	{env}_crm.cdlcrm.cdl_price_list
	{env}_crm.cdlcrm.cdl_product_cost
	{env}_crm.cdlcrm.cdl_mstr_pegrate
	{env}_crm.cdlcrm.cdl_mstr_taxrate
*/

--product lookup
drop table if exists {env}_crm.stage.tmp_coh_gc_skus1;
create table {env}_crm.stage.tmp_coh_gc_skus1 as
select 
  s.id as sku_id
, s.code as sku_code 
, case 
      when substring(s.style,1,2) = 'OC' and length(s.style) in (12,14) and strpos(s.style,'*') = 0 then substring(s.style,1,6)
      when substring(s.style,1,2) = 'OC' and length(s.style) in (12) and strpos(s.style,'*') > 0 then substring(s.style,3,3)
      when substring(s.style,1,2) = 'OC' and length(s.style) in (13) and strpos(s.style,'*') > 0 then substring(s.style,3,4)
      when substring(s.style,1,4) in ('CPN-', 'DPS_', 'F&F_') then substring(s.style,5,5)
      when substring(s.style,1,3) in ('OPR', 'CBE', 'CLE', 'CLR', 'CRC', 'DPS', 'GWP', 'JPO', 'LCE', 'MOD', 'OPU', 'P3P', 'PCE', 'PCI', 'PCO', 'PEM', 'PRS', 'PVI', 'VCH', 'VIO', 'VPB', 'VPO', 'VPU') then substring(s.style,1,6)
      when substring(s.style,7,3) = 'DUM' then substring(s.style,1,6)
      when substring(s.style,1,2) in ('FF', 'FS', 'GW', 'IR') then substring(s.style,1,6)
      when substring(s.style,1,2) = 'FL' then substring(s.style,3,6)
      else s.style end as style
, style_color 
, case 
      when substring(s.style,1,2) = 'OC' and length(s.style) in (12,14) and strpos(s.style,'*') = 0 then substring(s.style,7,5)
      when substring(s.style,1,4) in ('CPN-', 'DPS_', 'F&F_') then substring(s.style,11,5)
      when substring(s.style,1,3) in ('OPR', 'CBE', 'CLE', 'CLR', 'CRC', 'DPS', 'GWP', 'JPO', 'LCE', 'MOD', 'OPU', 'P3P', 'PCE', 'PCI', 'PCO', 'PEM', 'PRS', 'PVI', 'VCH', 'VIO', 'VPB', 'VPO', 'VPU') then substring(s.style,7,5)
      when substring(s.style,1,2) in ('FF', 'FS', 'GW', 'IR') then substring(s.style,7,5)
      when nvl(style_color,'') <> '' then substring(style_color,length(style)+1) 
      when nvl(size,'') <> '' then translate(substring(code,1,length(code)-length(size)), nvl(style,''),'')
      else translate(code, nvl(style,''),'')
      end as color
, case
      when nvl(size,'') <> '' then size
      when substring(s.style,1,2) = 'OC' and length(s.style) in (12,14) and strpos(s.style,'*') = 0 then substring(s.style,12)
      when substring(s.style,1,4) in ('CPN-', 'DPS_', 'F&F_') then substring(s.style,17)
      when substring(s.style,1,3) in ('OPR', 'CBE', 'CLE', 'CLR', 'CRC', 'DPS', 'GWP', 'JPO', 'LCE', 'MOD', 'OPU', 'P3P', 'PCE', 'PCI', 'PCO', 'PEM', 'PRS', 'PVI', 'VCH', 'VIO', 'VPB', 'VPO', 'VPU') then substring(s.style,12)
      when substring(s.style,7,3) = 'DUM' then substring(s.style,7)
      when substring(s.style,1,2) in ('FF', 'FS', 'GW', 'IR') then substring(s.style,12)
      else size end as size, upc_code
from {env}_crm.cdlcrm.cdl_mstr_product_sku s;

drop table if exists {env}_crm.stage.tmp_coh_gc_skus2;
create table {env}_crm.stage.tmp_coh_gc_skus2 as 
select sku_id, sku_code, trim(translate(style, '_', '')) as style,  trim(translate(color, '_', '')) as color, size, case when upc_code::varchar(50) = '0' then null else upc_code::varchar(50) end as upc
from {env}_crm.stage.tmp_coh_gc_skus1;

drop table if exists {env}_crm.stage.tmp_coh_gc_sku_lkp;
create table {env}_crm.stage.tmp_coh_gc_sku_lkp as 
with p as (select product_id, trim(translate(sty_code, '_', '')) as sty_code, color_code, sz_code, upc_code, sku from {env}_crm.cdlcrm.cdl_mstr_product where brand_code = 'COH')
select a.*, 
coalesce(p1.product_id, p2.product_id, p3.product_id, p4.product_id, p5.product_id,-9999) as product_id, 
coalesce(p1.match_int, p2.match_int, p3.match_int, p4.match_int, p5.match_int,6) as match_int
from {env}_crm.stage.tmp_coh_gc_skus2 a
left join (select 1 as match_int, upc_code, min(product_id) as product_id from p group by 1,2) as p1 on a.upc = p1.upc_code::varchar(50)
left join (select 2 as match_int, sty_code, color_code, sz_code, min(product_id) as product_id from p group by 1,2,3,4) as p2 on a.style = p2.sty_code and a.color = p2.color_code and a.size = p2.sz_code
left join (select 3 as match_int, sty_code, color_code, min(product_id) as product_id from p group by 1,2,3) as p3 on a.style = p3.sty_code and a.color = p3.color_code
left join (select 4 as match_int, sty_code, min(product_id) as product_id from p group by 1,2) as p4 on a.style = p4.sty_code
left join (select 5 as match_int, replace(replace(sku,' ',''),'_','') as sku, min(product_id) as product_id from p group by 1,2) as p5 on replace(replace(a.sku_code,' ',''),'_','') = replace(replace(p5.sku,' ',''),'_','');

/*end of sku to product lookup*/

--customer profile
drop table if exists {env}_crm.stage.tmp_coh_gc_customer_prof_base;
create table {env}_crm.stage.tmp_coh_gc_customer_prof_base as
select
c.customer_id,
c.birth_year,
cth.upgrade_date_time,
ctr.code as customer_type_code,
c.gender_code,
coalesce(case when p.country_code = 'OTHERS' then null else p.country_code end, c.exact_country) as residence_country,
row_number() over (partition by c.customer_id order by cth.upgrade_date_time desc) as rn
from (select * from {env}_crm.cdlcrm.cdl_customer where brand_code = 'Coach') c
left join (select * from {env}_crm.cdlcrm.cdl_customer_type_history where brand_code = 'Coach') cth on c.customer_id = cth.customer_id
left join {env}_crm.cdlcrm.cdl_mstr_cust_ref_customer_type ctr on c.customer_type = ctr.code
left join (select * from {env}_crm.cdlcrm.cdl_customer_address where brand_code = 'Coach') cty on c.customer_id = cty.customer_id
left join {env}_crm.cdlcrm.cdl_mstr_addr_ref_province p on cty.province_code = p.code
;

drop table if exists {env}_crm.stage.tmp_coh_gc_customer_prof_emp;
create table {env}_crm.stage.tmp_coh_gc_customer_prof_emp as
select
customer_id, birth_year, gender_code,
b.country_code as residence_country, 
max(case 
	when rn = 1 and customer_type_code = 'O' then 2
	when customer_type_code = 'O' then 1
	else 0
end) as emp_flag
from {env}_crm.stage.tmp_coh_gc_customer_prof_base a
left join {env}_crm.cdlcrm.lkp_residence_country b on a.residence_country = b.residence_country
group by 1,2,3,4
;

drop table if exists {env}_crm.dlabs.dlab_coh_gc_customer_profile;
create table {env}_crm.dlabs.dlab_coh_gc_customer_profile as
select 
customer_id, birth_year, gender_code as gender,
residence_country, 
case when emp_flag = 2 then 'E'
when emp_flag = 1 then 'F'
end as employee_status_code
from {env}_crm.stage.tmp_coh_gc_customer_prof_emp
;

--Start with all Coach Asia transactions at transcation/SKU level
drop table if exists {env}_crm.stage.tmp_coh_gc_01; 
create table {env}_crm.stage.tmp_coh_gc_01 distkey (txn_nbr) sortkey (customer_id) as
select p.id as txn_nbr, p.purchase_code as pur_code, p.txn_datetime::date as txn_date, p.txn_datetime as txn_datetime, nvl(p.customer_id,'-1') as customer_id, abs(ps.local_txn_amount)::numeric(10,2) as local_item_net_amt, ps.usd_txn_amount,
case when ps.item_sold_qty = 0 then 0 else abs(ps.local_txn_amount)/abs(ps.item_sold_qty)::numeric(10,2) end as local_aur, abs(ps.item_sold_qty)::numeric(10,2) as item_sold_qty, 
p.counter_code, 
sku.sku_id, sku.sku_code, sku.product_id, ps.retail_price, ps.return_flag
from {env}_crm.cdlcrm.cdl_transaction_hdr p
join {env}_crm.cdlcrm.cdl_transaction_dtl ps on ps.purchase_code = p.purchase_code
join {env}_crm.stage.tmp_coh_gc_sku_lkp sku on ps.sku_code = sku.sku_code
where nvl(ps.local_txn_amount,0) <> 0 and nvl(ps.item_sold_qty,0) <> 0;

--drop temp tables
--drop table if exists {env}_crm.stage.tmp_coh_gc_skus_lkp;

--Look up MSRP from EDW lookup and product details
drop table if exists {env}_crm.stage.tmp_coh_gc_02;
create table {env}_crm.stage.tmp_coh_gc_02 distkey (txn_nbr) sortkey (customer_id) as
select a.*, a.txn_nbr||'_'||a.sku_id||'_'||a.return_flag as txn_item_line_nbr, 
case when cc.group_code in (3,4) then 'RETAIL' when cc.id = 8 then 'OUTLET' when cc.id=100 then 'WHOLESALE' else '-1' end as src_channel_code,
case when cnt.code like '%OCE%' then 'ONLINE' when cnt.code = 'OCW11' then 'ONLINE' else 'STORE' end as src_sub_channel_code, 
case when a.return_flag = 0 then '1' else '2' end as sale_credit_code, 
'CS2K' as source_code, 'ACX' as cdi_source,
/*Local*/
case when a.retail_price > nvl(msrp.price,-1) then a.retail_price else msrp.price end as local_msrp,
case 
	when cnt.nationality_code in ('China', 'CHN') then 'CNY'
	when cnt.nationality_code in ('HKG','Hong Kong') then 'HKD'
	when cnt.nationality_code in ('MAC', 'Macau') then 'MOP'
	when cnt.nationality_code in ('SGP', 'Singapore') then 'SGD'
	when cnt.nationality_code in ('MYS', 'Malaysia') then 'MYR'
	when cnt.nationality_code in ('TWN', 'Taiwan') then 'TWD'
	when cnt.nationality_code in ('KOR', 'Korea') then 'KRW'
end as local_currency,
case 
	when cnt.nationality_code in ('China', 'CHN') then 'CN'
	when cnt.nationality_code in ('HKG','Hong Kong') then 'HK'
	when cnt.nationality_code in ('MAC', 'Macau') then 'MO'
	when cnt.nationality_code in ('SGP', 'Singapore') then 'SG'
	when cnt.nationality_code in ('MYS', 'Malaysia') then 'MY'
	when cnt.nationality_code in ('TWN', 'Taiwan') then 'TW'
	when cnt.nationality_code in ('KOR', 'Korea') then 'KR'
end as country_code, 
cnt.code as store_nbr,
--standardized product fields
p.dept_code, p.dept_desc, p.sty_code, p.color_code, p.collect_desc, p.silhouette_desc, p.handbag_sz, p.sz_code, p.class_desc, 
p.signature_type_desc, p.signature_type_mbr_desc, p.fact_type, p.sty_grp, p.sty_long_desc, p.mtrl_desc as material, p.mtrl_code,
p.color_grp,
--end standardized product fileds
case when p.product_identity = 'Product Identity Retail Derived' then 1 else 0 end as retail_derived,
case when p.fact_type not like '%Factory Exclusive%' and src_channel_code = 'OUTLET' then 1 else 0 end as retail_delete,
case when p.mtrl_desc like '%Exotics%' then 1 else 0 end as exotics,
case when p.spcl_product_mbr_desc like '%Novelty%' then 1 else 0 end as novelty,
case
	when p.dept_code in ('D01') then '1.Womens Bag'
	when p.dept_code in ('D02','D03','D22','D23') then '2.Womens Accessories'
	when p.dept_code in ('D11') then '3.Womens Footwear'
	when p.dept_code in ('D07') then '4.Womens RTW'
	when p.dept_code in ('D06','D09','D12','D13','D14','D15','D17','N/A') then '5.Womens Other'
	when p.dept_code in ('D04','D05','D08','D10','D16','D18','D19','D20','D21') then '6.Mens'
	else '7.Other'
end as category,
case when src_channel_code = 'RETAIL' and (p.sty_long_desc like '%1941%' or p.collect_desc like '%1941%') then 1 else 0 end as flag_1941
from {env}_crm.stage.tmp_coh_gc_01 a
join (select distinct code,counter_category_code,nationality_code from {env}_crm.cdlcrm.cdl_mstr_counter) cnt on cnt.code = a.counter_code
join {env}_crm.cdlcrm.cdl_mstr_counter_category cc on cnt.counter_category_code = cc.code
left join {env}_crm.cdlcrm.cdl_mstr_product p on a.product_id = p.product_id and a.product_id <> -9999
left join {env}_crm.cdlcrm.cdl_price_list msrp on a.product_id = msrp.product_key and a.txn_date between msrp.start_date and msrp.end_date and
case
		when cnt.nationality_code in ('China', 'CHN') then 4
		when cnt.nationality_code in ('HKG','Hong Kong') then 5
		when cnt.nationality_code in ('MAC', 'Macau') then 13
		when cnt.nationality_code in ('SGP', 'Singapore') then 9
		when cnt.nationality_code in ('MYS', 'Malaysia') then 8
		when cnt.nationality_code in ('TWN', 'Taiwan') then 11
		when cnt.nationality_code in ('KOR', 'Korea') then 7
	end = msrp.price_list_key
;

--drop temp tables
--drop table if exists {env}_crm.stage.tmp_coh_gc_01;

/*  Perform FX and remove tax and remove virtual store*/
drop table if exists {env}_crm.stage.tmp_coh_gc_03;
create table {env}_crm.stage.tmp_coh_gc_03 distkey (txn_nbr) sortkey (customer_id) as
select a.*,
case when peg.peg_rate is null then null else a.local_msrp / nvl(tax.tax_rate,1) / nvl(peg.peg_rate,1) end as usd_msrp,
case when peg.peg_rate is null then a.usd_txn_amount else (a.local_item_net_amt / nvl(tax.tax_rate,1) / nvl(peg.peg_rate,1))::numeric(15,2) end as usd_item_net_amt,
case when a.item_sold_qty = 0 then 0 else (case when peg.peg_rate is null then a.usd_txn_amount else (a.local_item_net_amt / nvl(tax.tax_rate,1) / nvl(peg.peg_rate,1))::numeric(15,2) end /abs(a.item_sold_qty))::numeric(15,2) end as usd_aur,
--discount
case when nvl(a.item_sold_qty,0) = 0 then -1
	 when nvl(a.local_msrp,0) = 0 then 0
	 else case when (nvl(a.local_item_net_amt,0)/a.item_sold_qty)/a.local_msrp <= 0.9 then 1 else 0 end 
end as discount_flag, -- when paid amount is within 90% of msrp then full price else discounted
case when nvl(a.local_msrp,0) = 0 then 0
	 else (1-(nvl(a.local_item_net_amt,0)/a.item_sold_qty)/a.local_msrp)*100 
end as discount_pct,
c.fisc_year_num, 
(c.fisc_year_num * 100) + c.fisc_quarter_num as fisc_year_quarter, 
(c.fisc_year_num * 100) + c.fisc_month_num as fisc_year_month, 
(c.fisc_year_num * 100) + c.fisc_week_of_year_num as fisc_year_week, 
(c.fisc_year_num * 1000) + c.fisc_year_day_num as fisc_year_day
from {env}_crm.stage.tmp_coh_gc_02 a
join {env}_crm.cdlcrm.cdl_mstr_calendar c on a.txn_date = c.calendar_date
left join {env}_crm.cdlcrm.cdl_mstr_pegrate peg on c.fisc_year_num = peg.fisc_year_num and c.fisc_year_day_num between peg.start_day_num and case when peg.end_day_num = 364 then 371 else peg.end_day_num end and a.local_currency = peg.currency
left join {env}_crm.cdlcrm.cdl_mstr_taxrate tax on a.txn_date between tax.start_date and tax.end_date and a.country_code = tax.country_code
where a.country_code in ('CN', 'HK', 'MO', 'TW') and store_nbr not in ('OCE23', 'OCE25', 'OCE27', 'OCE37', 'OCE43', 'OCE44', 'OCE45', 'OCE47', 'OCE62', 'OCE63', 'OCE64', 'OCE68')
;

--drop temp tables
--drop table if exists {env}_crm.stage.tmp_coh_gc_02;

--Look up store details
drop table if exists {env}_crm.stage.tmp_coh_gc_04;
create table {env}_crm.stage.tmp_coh_gc_04 distkey (txn_nbr) sortkey (customer_id) as
select
a.*,
loc.location_id, loc.door_desc as location_desc, loc.region_desc, loc.district_desc, loc.city, loc.state,
c.fisc_year_num as comp_year_ty, 
nvl(cty.comp_category_desc,'NON-COMP') as comp_ind_ty, 
c.fisc_year_num + 1 as comp_year_ly, 
nvl(cly.comp_category_desc,'NON-COMP') as comp_ind_ly
from {env}_crm.stage.tmp_coh_gc_03 a
left join {env}_crm.cdlcrm.cdl_mstr_location loc on a.country_code||a.store_nbr = loc.door_code and upper(loc.store_nbr)not like '%VIRTUAL%' and loc.sap_site_id <> 'N/A' and loc.location_id <> 1256736
left join {env}_crm.cdlcrm.cdl_mstr_calendar c on a.txn_date = c.calendar_date
left join {env}_crm.cdlcrm.hdw_lkp_comp_ind_mst cty on 
	loc.sap_site_id = cty.sap_site_id and 
	c.fisc_year_num = cty.cal_year_num and 
	c.fisc_year_day_num between cty.start_day_num and cty.end_day_num and 
	case when c.fisc_year_num - 1 in (select fisc_year_num from {env}_crm.dlabs.dlab_sys_shifted_calendar_years) then 4 else 1 end = cty.cal_key
	and cty.year_mode = 'TY'
left join {env}_crm.cdlcrm.hdw_lkp_comp_ind_mst cly on 
	loc.sap_site_id = cly.sap_site_id and 
	c.fisc_year_num = cly.cal_year_num and 
	case when c.fisc_year_num in (select fisc_year_num from {env}_crm.dlabs.dlab_sys_shifted_calendar_years) then c.fisc_year_day_num - 7 else c.fisc_year_day_num end between cly.start_day_num and cly.end_day_num and 
	case when c.fisc_year_num in (select fisc_year_num from {env}_crm.dlabs.dlab_sys_shifted_calendar_years) then 4 else 1 end = cly.cal_key
	and cly.year_mode = 'LY'
;

--drop temp tables
--drop table if exists {env}_crm.stage.tmp_coh_gc_03;


--TXN order lookup
drop table if exists {env}_crm.stage.tmp_coh_gc_txn_order;
create table {env}_crm.stage.tmp_coh_gc_txn_order distkey (txn_nbr) sortkey (customer_id) as
with a as (
	select customer_id, 
	txn_nbr, 
	txn_datetime,
	first_value(txn_datetime) over (partition by customer_id order by customer_id, txn_datetime rows between unbounded preceding and unbounded following) as cust_first_txn_date, 
	dense_rank() over (partition by customer_id order by customer_id, txn_datetime) as drn, 
	rank() over (partition by customer_id order by customer_id, txn_datetime) as rn
	from 
	(
		select customer_id, txn_nbr, min(txn_datetime) as txn_datetime
		from {env}_crm.stage.tmp_coh_gc_04 t
		where t.customer_id <> -1 and t.country_code in ('CN', 'HK', 'MO', 'TW') and nvl(local_item_net_amt,0) <> 0
		group by 1,2
	) as foo
)
select a.customer_id, a.txn_nbr, a.cust_first_txn_date, b.txn_datetime as cust_prev_txn_datetime, (a.txn_datetime::date - b.txn_datetime::date) as days_since_cust_prev_txn, a.rn, a.drn
from a left join (select distinct customer_id, drn, txn_datetime from a ) as b on a.customer_id = b.customer_id and a.drn-1 = b.drn
order by 1,5
;

--Purchase order lookup
drop table if exists {env}_crm.stage.tmp_coh_gc_pur_order;
create table {env}_crm.stage.tmp_coh_gc_pur_order distkey (txn_nbr) sortkey (customer_id) as
with a as (
	select customer_id, 
	txn_nbr, 
	txn_datetime,
	first_value(txn_datetime) over (partition by customer_id order by customer_id, txn_datetime rows between unbounded preceding and unbounded following) as cust_first_pur_date, 
	dense_rank() over (partition by customer_id order by customer_id, txn_datetime) as drn, 
	rank() over (partition by customer_id order by customer_id, txn_datetime) as rn
	from 
	(
		select customer_id, txn_nbr, min(txn_datetime) as txn_datetime
		from {env}_crm.stage.tmp_coh_gc_04 t
		where t.customer_id <> -1 and t.sale_credit_code = '1' and t.country_code in ('CN', 'HK', 'MO', 'TW') and nvl(local_item_net_amt,0) <> 0
		group by 1,2
	) as foo
)
select a.customer_id, a.txn_nbr, a.cust_first_pur_date, b.txn_datetime as cust_prev_pur_datetime, (a.txn_datetime::date - b.txn_datetime::date) as days_since_cust_prev_pur, a.rn, a.drn
from a left join (select distinct customer_id, drn, txn_datetime from a ) as b on a.customer_id = b.customer_id and a.drn-1 = b.drn
order by 1,5
;


---Create customer first purchase date transaction field
drop table if exists {env}_crm.stage.tmp_coh_gc_cust_first_pur_dt_txns;
create table {env}_crm.stage.tmp_coh_gc_cust_first_pur_dt_txns as
select distinct pur_trans.customer_id as customer_id, pur_trans.txn_nbr
from 
(
select distinct customer_id, txn_nbr, txn_date
from {env}_crm.stage.tmp_coh_gc_04
where customer_id <> -1 and sale_credit_code = '1' and country_code in ('CN', 'HK', 'MO', 'TW') and nvl(local_item_net_amt,0) <> 0
) as pur_trans
join (
select customer_id, min(txn_date) as first_purchase_date
from {env}_crm.stage.tmp_coh_gc_04
where customer_id <> -1 and sale_credit_code = '1' and country_code in ('CN', 'HK', 'MO', 'TW') and nvl(local_item_net_amt,0) <> 0
group by 1
) as fp on pur_trans.customer_id = fp.customer_id and pur_trans.txn_date = fp.first_purchase_date
;



--Final adjustments (rounding and setting credits to negatives)
drop table if exists {env}_crm.stage.tmp_coh_gc_txn;
create table {env}_crm.stage.tmp_coh_gc_txn distkey (txn_nbr) sortkey (txn_date, location_id) as
select distinct
coalesce(b.customer_id, a.customer_id) as customer_id, 
txn_dt.cust_first_txn_date,
pur_dt.cust_first_pur_date,
case when fpt.txn_nbr is not null then 1 else 0 end as cust_first_pur_dt_txns_flag,

b.rn as customer_txn_num, case when b.drn = 1 then 1 else 0 end as cust_first_txn,
b.cust_prev_txn_datetime,
b.days_since_cust_prev_txn,

c.rn as customer_pur_num, case when c.drn = 1 then 1 else 0 end as cust_first_pur,
c.cust_prev_pur_datetime,
c.days_since_cust_prev_pur,

a.cdi_source, a.txn_nbr::nvarchar(40) as txn_nbr,a.pur_code, a.txn_item_line_nbr::nvarchar(50) as txn_item_line_nbr, a.txn_date::date as txn_date, a.txn_datetime::datetime as txn_datetime, a.src_channel_code, a.src_sub_channel_code, 
case 
	when a.src_channel_code = 'RETAIL' and a.src_sub_channel_code = 'STORE' then 'RETAIL'
	when a.src_channel_code = 'RETAIL' and a.src_sub_channel_code = 'ONLINE' then 'ECOM'
	when a.src_channel_code = 'OUTLET' and a.src_sub_channel_code = 'STORE' then 'OUTLET'
	when a.src_channel_code = 'OUTLET' and a.src_sub_channel_code = 'ONLINE' then 'EOS'
	when a.src_channel_code = 'WHOLESALE' and a.src_sub_channel_code = 'ONLINE' then 'WHOLESALE ECOM'

end as channel_name,
a.sale_credit_code, a.source_code, case when a.sale_credit_code = '1' then a.item_sold_qty else -1 * a.item_sold_qty end as item_sold_qty,  
round(a.local_msrp,2) as local_msrp, 
case when a.sale_credit_code = '2' then round(-1*abs(a.local_item_net_amt),2) else round(abs(a.local_item_net_amt),2) end as local_item_net_amt, 
case when a.sale_credit_code = '2' then round(-1*abs(a.local_aur),2) else round(abs(a.local_aur),2) end as local_aur, a.local_currency, 
round(a.usd_msrp,2) as usd_msrp, 
case when a.sale_credit_code = '2' then round(-1*abs(a.usd_item_net_amt),2) else round(abs(a.usd_item_net_amt),2) end as usd_item_net_amt, 
case when a.sale_credit_code = '2' then round(-1*abs(a.usd_aur),2) else round(abs(a.usd_aur),2) end as usd_aur, 
case when a.sale_credit_code = '2' then -1 * cost.curr_std_cst_usd else cost.curr_std_cst_usd end as usd_auc, 
case when a.item_sold_qty > 0 and a.usd_item_net_amt > 0 then round((abs(a.usd_item_net_amt/a.item_sold_qty) - cost.curr_std_cst_usd)/abs(a.usd_item_net_amt/a.item_sold_qty),2) * 100 else null end as gross_margin,
a.discount_flag, round(a.discount_pct,2) as discount_pct,
-1 as pos_porter_flag,
a.location_id, a.country_code, a.store_nbr, case when a.store_nbr = 'OCW11' then 'VIPWHOLESALE' else a.location_desc end as location_desc, a.region_desc, a.district_desc, a.city, a.state,
a.product_id, a.dept_code, a.dept_desc, a.sty_code, a.color_code, a.collect_desc, a.silhouette_desc, a.handbag_sz, a.sz_code, a.class_desc, 
a.signature_type_desc, a.signature_type_mbr_desc, a.color_grp, a.sty_long_desc, a.material, a.mtrl_code, a.fact_type, a.sty_grp, a.category,
a.retail_derived, a.retail_delete, a.exotics, a.novelty, a.flag_1941,
a.fisc_year_num, a.fisc_year_quarter, a.fisc_year_month, a.fisc_year_week, a.fisc_year_day, 
a.comp_year_ty, a.comp_ind_ty, 
a.comp_year_ly, a.comp_ind_ly,
current_date as md_created_date
from {env}_crm.stage.tmp_coh_gc_04 a
left join {env}_crm.stage.tmp_coh_gc_cust_first_pur_dt_txns fpt on a.customer_id = fpt.customer_id and a.txn_nbr = fpt.txn_nbr
left join {env}_crm.stage.tmp_coh_gc_txn_order b on a.txn_nbr = b.txn_nbr
left join {env}_crm.stage.tmp_coh_gc_pur_order c on a.txn_nbr = c.txn_nbr
left join {env}_crm.cdlcrm.cdl_product_cost cost on a.product_id = cost.product_key and a.txn_date between cost.start_date and cost.end_date
left join (select customer_id, cust_first_txn_date from {env}_crm.stage.tmp_coh_gc_txn_order group by 1,2) as txn_dt on b.customer_id = txn_dt.customer_id
left join (select customer_id, cust_first_pur_date from {env}_crm.stage.tmp_coh_gc_pur_order group by 1,2) as pur_dt on b.customer_id = pur_dt.customer_id
where country_code in ('CN', 'HK', 'MO', 'TW')
;

--drop temp tables
--drop table if exists {env}_crm.stage.tmp_coh_gc_04;
--drop table if exists {env}_crm.stage.tmp_coh_gc_txn_order;
--drop table if exists {env}_crm.stage.tmp_coh_gc_pur_order;
--drop table if exists {env}_crm.stage.tmp_coh_gc_cust_first_pur_dt_txns;

--alter tmp table
drop table if exists {env}_crm.dlabs.dlab_coh_gc_txn;
create table {env}_crm.dlabs.dlab_coh_gc_txn (like {env}_crm.stage.tmp_coh_gc_txn);
--alter table {env}_crm.dlabs.dlab_coh_gc_txn append from {env}_crm.stage.tmp_coh_gc_txn;
--alter table append is not allowed in transaction block
insert into {env}_crm.dlabs.dlab_coh_gc_txn (select * from {env}_crm.stage.tmp_coh_gc_txn);
--drop table if exists {env}_crm.stage.tmp_coh_gc_txn;
