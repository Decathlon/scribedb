 CREATE or REPLACE FUNCTION HASH_MD5 (
    psINPUT IN VARCHAR2
    ) RETURN VARCHAR2 AS
    rHash RAW (16);
    BEGIN
    rHash := DBMS_CRYPTO.HASH (TO_CLOB (psINPUT), DBMS_CRYPTO.hash_md5);
    RETURN (LOWER (RAWTOHEX (rHash)));
    END HASH_MD5;
    /
    
select sum(nb) from (    
select hash_md5(CPM||COMMERCIAL_SEASON_ID||PURCHASE_ESTIMATED_QUANTITY||R3_MODEL_CODE) md5,to_number(hash_md5(CPM||COMMERCIAL_SEASON_ID||PURCHASE_ESTIMATED_QUANTITY||R3_MODEL_CODE),'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') nb
 from brandata.reprise_estimated) a


select ora_hash(CPM||COMMERCIAL_SEASON_ID||PURCHASE_ESTIMATED_QUANTITY||R3_MODEL_CODE)
 from brandata.reprise_estimated 


Select CRC32(CPM||COMMERCIAL_SEASON_ID||PURCHASE_ESTIMATED_QUANTITY||R3_MODEL_CODE) from brandata.reprise_estimated 

 
 select hash_md5(CPM)
 from brandata.reprise_estimated;

1d7e2d8f5c7c88962f26d70e30e4dbe6
select * from brandata.reprise_estimated;

select sum(nb) from (    
select hash_md5(c1||c2||c3),'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') nb
 from toto) a

with q1 as (select hash_md5(CPM||COMMERCIAL_SEASON_ID||PURCHASE_ESTIMATED_QUANTITY||R3_MODEL_CODE) md5,cpm from brandata.reprise_estimated)
select q1.md5, hash_md5(q1.md5||LAG(q1.md5,1,0)) OVER (order by q1.md5)
from q1

with q1 as (select hash_md5(CPM||COMMERCIAL_SEASON_ID||PURCHASE_ESTIMATED_QUANTITY||R3_MODEL_CODE) md5,md5||LAG(q1.md5,1,0)) OVER (order by q1.md5) from brandata.reprise_estimated)
select q1.md5, q1.md5||LAG(q1.md5,1,0) OVER (order by q1.md5)
from q1





select md5, md5(md5||LAG(md5,1,0)) OVER (order by c1)
from brandata.reprise_estimated;


select * from dual


WITH Factorial (operand,total_so_far) AS
(SELECT 5 operand, 5 total_so_far FROM dual    -- Using anchor member to pass in "5"
UNION ALL
SELECT operand-1, total_so_far * (operand-1) FROM Factorial
WHERE operand > 1)
SELECT * FROM Factorial;

WITH Factorial (operand,total_so_far) AS
(SELECT 5 operand, 5 total_so_far FROM dual    -- Find the factorial of 5
UNION ALL
SELECT operand-1, total_so_far * (operand-1) FROM Factorial
WHERE operand > 1)
SELECT MAX(operand) || '! = ' || MAX(total_so_far) AS RESULT FROM Factorial;

with Factorial (operand,total_so_far) AS 
(select cpm operand,hash_md5(CPM||COMMERCIAL_SEASON_ID||PURCHASE_ESTIMATED_QUANTITY||R3_MODEL_CODE) total_so_far from brandata.reprise_estimated
union all
SELECT operand-1, total_so_far * (operand-1) FROM Factorial




select BDE_ID_BOM_DETAIL,
BHD_ID_BOM_HEADER_BHD,
BDE_TPR_SUB_THIRD_PAR_NUM_BHD,
BDE_TPR_THIRD_PAR_NUM_BHD,
BDE_TPT_THI_PAR_NUM_TYP_BHD,
md5(BDE_ID_BOM_DETAIL::text||BHD_ID_BOM_HEADER_BHD::text||BDE_TPR_SUB_THIRD_PAR_NUM_BHD::text||BDE_TPR_THIRD_PAR_NUM_BHD::text||BDE_TPT_THI_PAR_NUM_TYP_BHD::text) as md5
from  prdrefc_iso.bom_details
;

select r1.BDE_ID_BOM_DETAIL,r1.BHD_ID_BOM_HEADER_BHD,r1.BDE_TPR_SUB_THIRD_PAR_NUM_BHD,r1.BDE_TPR_THIRD_PAR_NUM_BHD,r1.BDE_TPT_THI_PAR_NUM_TYP_BHD,
r1.md5, 
md5(r1.md5||r1.last) as result from
(select BDE_ID_BOM_DETAIL,
BHD_ID_BOM_HEADER_BHD,
BDE_TPR_SUB_THIRD_PAR_NUM_BHD,
BDE_TPR_THIRD_PAR_NUM_BHD,
BDE_TPT_THI_PAR_NUM_TYP_BHD,
md5(BDE_ID_BOM_DETAIL||BHD_ID_BOM_HEADER_BHD||BDE_TPR_SUB_THIRD_PAR_NUM_BHD||BDE_TPR_THIRD_PAR_NUM_BHD||BDE_TPT_THI_PAR_NUM_TYP_BHD) as md5,
lag(BDE_ID_BOM_DETAIL||BHD_ID_BOM_HEADER_BHD||BDE_TPR_SUB_THIRD_PAR_NUM_BHD||BDE_TPR_THIRD_PAR_NUM_BHD||BDE_TPT_THI_PAR_NUM_TYP_BHD,1)  over (order by BDE_ID_BOM_DETAIL,
BHD_ID_BOM_HEADER_BHD,
BDE_TPR_SUB_THIRD_PAR_NUM_BHD,
BDE_TPR_THIRD_PAR_NUM_BHD,
BDE_TPT_THI_PAR_NUM_TYP_BHD) as last from prdrefc_iso.bom_details limit 5) as r1;





select table_result.result from
 (
select r1.BDE_ID_BOM_DETAIL,r1.BHD_ID_BOM_HEADER_BHD,r1.BDE_TPR_SUB_THIRD_PAR_NUM_BHD,r1.BDE_TPR_THIRD_PAR_NUM_BHD,r1.BDE_TPT_THI_PAR_NUM_TYP_BHD,
r1.md5, 
md5(r1.md5||r1.last) as result 
 from
   (
select BDE_ID_BOM_DETAIL,
BHD_ID_BOM_HEADER_BHD,
BDE_TPR_SUB_THIRD_PAR_NUM_BHD,
BDE_TPR_THIRD_PAR_NUM_BHD,
BDE_TPT_THI_PAR_NUM_TYP_BHD,
md5(
    coalesce(bhd_id_bom_header_bhd::text,'')||
    coalesce(bde_tpt_thi_par_num_typ_bhd::text,'')||
    coalesce(bde_tpr_third_par_num_bhd::text,'')||
    coalesce(bde_tpr_sub_third_par_num_bhd::text,'')||
    coalesce(bde_id_bom_detail::text,'')||
    coalesce(bde_tpt_thi_par_num_typ_ref::text,'')||
    coalesce(bde_tpr_third_par_num_ref::text,'')||
    coalesce(bde_tpr_sub_third_par_num_ref::text,'')||
    coalesce(bde_label_component_desc::text,'')||
    coalesce(bgp_id_bom_group_bgp::text,'')||
    coalesce(bgp_label_group_desc_bgp::text,'')||
    coalesce(bsg_id_bom_sub_group_bsg::text,'')||
    coalesce(bsg_label_sub_group_desc_bsg::text,'')||
    coalesce(sta_code_sta::text,'')||
    coalesce(gty_grid_type_num_grd_bsv::text,'')||
    coalesce(gva_grid_val_bsv::text,'')||
    coalesce(bde_flag_compulsory::text,'')||
    coalesce(bde_quantity_consummate::text,'')||
    coalesce(bde_label_comment::text,'')||
    coalesce(mun_category_unit_rmu::text,'')||
    coalesce(mun_usage_unit_rmu::text,'')||
    coalesce(bde_year_usage_beginning::text,'')||
    coalesce(bde_week_usage_beginning::text,'')||
    coalesce(bde_year_usage_ending::text,'')||
    coalesce(bde_week_usage_ending::text,'')||
    coalesce(bde_id_profile_create::text,'')||
    coalesce(bde_date_create::text,'')||
    coalesce(bde_id_profile_update::text,'')||
    coalesce(bde_date_update::text,'')||
    coalesce(bde_line_type::text,'')||
    coalesce(bde_technical_file_ref::text,'')||
    coalesce(fpc_purch_price_cost::text,'')||
    coalesce(cur_currency_code_pur::text,'')||
    coalesce(fpc_pcb::text,'')||
    coalesce(fpl_replenishment_ldt::text,'')||
    coalesce(fpl_qt_order_min::text,'')||
    coalesce(bde_tpt_thi_par_num_typ_sel::text,'')||
    coalesce(bde_tpr_third_par_num_sel::text,'')||
    coalesce(bde_tpr_sub_third_par_num_sel::text,'')||
    coalesce(bde_cof_ref_code_ref::text,'')||
    coalesce(cof_ref_code_ref::text,'')||
    coalesce(elp_element_prod_num_elp::text,'')||
    coalesce(tpt_thi_par_num_typ_bhd::text,'')||
    coalesce(tpr_third_par_num_bhd::text,'')||
    coalesce(tpr_sub_third_par_num_bhd::text,'')||
    coalesce(tpt_thi_par_num_typ_ref::text,'')||
    coalesce(tpr_third_par_num_ref::text,'')||
    coalesce(tpr_sub_third_par_num_ref::text,'')||
    coalesce(tpt_thi_par_num_typ_sel::text,'')||
    coalesce(tpr_third_par_num_sel::text,'')||
    coalesce(tpr_sub_third_par_num_sel::text,'')
    ) as md5,
lag( 
    coalesce(bhd_id_bom_header_bhd::text,'')||
    coalesce(bde_tpt_thi_par_num_typ_bhd::text,'')||
    coalesce(bde_tpr_third_par_num_bhd::text,'')||
    coalesce(bde_tpr_sub_third_par_num_bhd::text,'')||
    coalesce(bde_id_bom_detail::text,'')||
    coalesce(bde_tpt_thi_par_num_typ_ref::text,'')||
    coalesce(bde_tpr_third_par_num_ref::text,'')||
    coalesce(bde_tpr_sub_third_par_num_ref::text,'')||
    coalesce(bde_label_component_desc::text,'')||
    coalesce(bgp_id_bom_group_bgp::text,'')||
    coalesce(bgp_label_group_desc_bgp::text,'')||
    coalesce(bsg_id_bom_sub_group_bsg::text,'')||
    coalesce(bsg_label_sub_group_desc_bsg::text,'')||
    coalesce(sta_code_sta::text,'')||
    coalesce(gty_grid_type_num_grd_bsv::text,'')||
    coalesce(gva_grid_val_bsv::text,'')||
    coalesce(bde_flag_compulsory::text,'')||
    coalesce(bde_quantity_consummate::text,'')||
    coalesce(bde_label_comment::text,'')||
    coalesce(mun_category_unit_rmu::text,'')||
    coalesce(mun_usage_unit_rmu::text,'')||
    coalesce(bde_year_usage_beginning::text,'')||
    coalesce(bde_week_usage_beginning::text,'')||
    coalesce(bde_year_usage_ending::text,'')||
    coalesce(bde_week_usage_ending::text,'')||
    coalesce(bde_id_profile_create::text,'')||
    coalesce(bde_date_create::text,'')||
    coalesce(bde_id_profile_update::text,'')||
    coalesce(bde_date_update::text,'')||
    coalesce(bde_line_type::text,'')||
    coalesce(bde_technical_file_ref::text,'')||
    coalesce(fpc_purch_price_cost::text,'')||
    coalesce(cur_currency_code_pur::text,'')||
    coalesce(fpc_pcb::text,'')||
    coalesce(fpl_replenishment_ldt::text,'')||
    coalesce(fpl_qt_order_min::text,'')||
    coalesce(bde_tpt_thi_par_num_typ_sel::text,'')||
    coalesce(bde_tpr_third_par_num_sel::text,'')||
    coalesce(bde_tpr_sub_third_par_num_sel::text,'')||
    coalesce(bde_cof_ref_code_ref::text,'')||
    coalesce(cof_ref_code_ref::text,'')||
    coalesce(elp_element_prod_num_elp::text,'')||
    coalesce(tpt_thi_par_num_typ_bhd::text,'')||
    coalesce(tpr_third_par_num_bhd::text,'')||
    coalesce(tpr_sub_third_par_num_bhd::text,'')||
    coalesce(tpt_thi_par_num_typ_ref::text,'')||
    coalesce(tpr_third_par_num_ref::text,'')||
    coalesce(tpr_sub_third_par_num_ref::text,'')||
    coalesce(tpt_thi_par_num_typ_sel::text,'')||
    coalesce(tpr_third_par_num_sel::text,'')||
    coalesce(tpr_sub_third_par_num_sel::text,'')
    ,1) over (order by BDE_ID_BOM_DETAIL,
BHD_ID_BOM_HEADER_BHD asc,
BDE_TPR_SUB_THIRD_PAR_NUM_BHD asc,
BDE_TPR_THIRD_PAR_NUM_BHD asc ,
BDE_TPT_THI_PAR_NUM_TYP_BHD asc) as last,
row_number() over (order by  BDE_ID_BOM_DETAIL,
BHD_ID_BOM_HEADER_BHD asc,
BDE_TPR_SUB_THIRD_PAR_NUM_BHD asc,
BDE_TPR_THIRD_PAR_NUM_BHD asc ,
BDE_TPT_THI_PAR_NUM_TYP_BHD asc) as numrow
from prdrefc_iso.bom_details limit 5) r1
order by r1.numrow desc) table_result
limit 1

md5( rpad(coalesce(bde_date_create::text,''),26,'0')



oracle : 
select table_result.result from
 (
select r1.BDE_ID_BOM_DETAIL,r1.BHD_ID_BOM_HEADER_BHD,r1.BDE_TPR_SUB_THIRD_PAR_NUM_BHD,r1.BDE_TPR_THIRD_PAR_NUM_BHD,r1.BDE_TPT_THI_PAR_NUM_TYP_BHD,
r1.md5, 
hash_md5(r1.md5||r1.last) as result 
 from
   (
select BDE_ID_BOM_DETAIL,
BHD_ID_BOM_HEADER_BHD,
BDE_TPR_SUB_THIRD_PAR_NUM_BHD,
BDE_TPR_THIRD_PAR_NUM_BHD,
BDE_TPT_THI_PAR_NUM_TYP_BHD,
hash_md5(
    bhd_id_bom_header_bhd||
    bde_tpt_thi_par_num_typ_bhd||
    bde_tpr_third_par_num_bhd||
    bde_tpr_sub_third_par_num_bhd||
    bde_id_bom_detail||
    bde_tpt_thi_par_num_typ_ref||
    bde_tpr_third_par_num_ref||
    bde_tpr_sub_third_par_num_ref||
    bde_label_component_desc||
    bgp_id_bom_group_bgp||
    bgp_label_group_desc_bgp||
    bsg_id_bom_sub_group_bsg||
    bsg_label_sub_group_desc_bsg||
    sta_code_sta||
    gty_grid_type_num_grd_bsv||
    gva_grid_val_bsv||
    bde_flag_compulsory||
    bde_quantity_consummate||
    bde_label_comment||
    mun_category_unit_rmu||
    mun_usage_unit_rmu||
    bde_year_usage_beginning||
    bde_week_usage_beginning||
    bde_year_usage_ending||
    bde_week_usage_ending||
    bde_id_profile_create||
    bde_date_create||
    bde_id_profile_update||
    bde_date_update||
    bde_line_type||
    bde_technical_file_ref||
    fpc_purch_price_cost||
    cur_currency_code_pur||
    fpc_pcb||
    fpl_replenishment_ldt||
    fpl_qt_order_min||
    bde_tpt_thi_par_num_typ_sel||
    bde_tpr_third_par_num_sel||
    bde_tpr_sub_third_par_num_sel||
    bde_cof_ref_code_ref||
    cof_ref_code_ref||
    elp_element_prod_num_elp||
    tpt_thi_par_num_typ_bhd||
    tpr_third_par_num_bhd||
    tpr_sub_third_par_num_bhd||
    tpt_thi_par_num_typ_ref||
    tpr_third_par_num_ref||
    tpr_sub_third_par_num_ref||
    tpt_thi_par_num_typ_sel||
    tpr_third_par_num_sel||
    tpr_sub_third_par_num_sel
    ) as md5,
lag( 
    bhd_id_bom_header_bhd||
    bde_tpt_thi_par_num_typ_bhd||
    bde_tpr_third_par_num_bhd||
    bde_tpr_sub_third_par_num_bhd||
    bde_id_bom_detail||
    bde_tpt_thi_par_num_typ_ref||
    bde_tpr_third_par_num_ref||
    bde_tpr_sub_third_par_num_ref||
    bde_label_component_desc||
    bgp_id_bom_group_bgp||
    bgp_label_group_desc_bgp||
    bsg_id_bom_sub_group_bsg||
    bsg_label_sub_group_desc_bsg||
    sta_code_sta||
    gty_grid_type_num_grd_bsv||
    gva_grid_val_bsv||
    bde_flag_compulsory||
    bde_quantity_consummate||
    bde_label_comment||
    mun_category_unit_rmu||
    mun_usage_unit_rmu||
    bde_year_usage_beginning||
    bde_week_usage_beginning||
    bde_year_usage_ending||
    bde_week_usage_ending||
    bde_id_profile_create||
    bde_date_create||
    bde_id_profile_update||
    bde_date_update||
    bde_line_type||
    bde_technical_file_ref||
    fpc_purch_price_cost||
    cur_currency_code_pur||
    fpc_pcb||
    fpl_replenishment_ldt||
    fpl_qt_order_min||
    bde_tpt_thi_par_num_typ_sel||
    bde_tpr_third_par_num_sel||
    bde_tpr_sub_third_par_num_sel||
    bde_cof_ref_code_ref||
    cof_ref_code_ref||
    elp_element_prod_num_elp||
    tpt_thi_par_num_typ_bhd||
    tpr_third_par_num_bhd||
    tpr_sub_third_par_num_bhd||
    tpt_thi_par_num_typ_ref||
    tpr_third_par_num_ref||
    tpr_sub_third_par_num_ref||
    tpt_thi_par_num_typ_sel||
    tpr_third_par_num_sel||
    tpr_sub_third_par_num_sel
    ,1) over (order by BDE_ID_BOM_DETAIL,
BHD_ID_BOM_HEADER_BHD asc,
BDE_TPR_SUB_THIRD_PAR_NUM_BHD asc,
BDE_TPR_THIRD_PAR_NUM_BHD asc ,
BDE_TPT_THI_PAR_NUM_TYP_BHD asc) as last,
row_number() over (order by  BDE_ID_BOM_DETAIL,
BHD_ID_BOM_HEADER_BHD asc,
BDE_TPR_SUB_THIRD_PAR_NUM_BHD asc,
BDE_TPR_THIRD_PAR_NUM_BHD asc ,
BDE_TPT_THI_PAR_NUM_TYP_BHD asc) as numrow
from prdrefc_iso.bom_details where rownum<=5) r1
order by r1.numrow desc) table_result
where rownum=1


create database orders_as1_iso;
\c orders_as1_iso
create schema orders_as1_iso;
CREATE TABLE orders_as1_iso.order_comment (
	org_num_organization_ord int4 NOT NULL,
	ord_order_number_ord int8 NOT NULL,
	cmt_comment_id int8 NOT NULL,
	cmt_comment varchar(255) NULL,
	cmt_creation_date timestamp NULL,
	cmt_update_date timestamp NULL,
	cmt_updater_profile varchar(255) NULL,
	cmt_private_flag bpchar(1) NULL,
	cmt_creation_in_extranet_flag bpchar(1) NULL,
	cmt_read_by_supplier_flag bpchar(1) NULL,
	cmt_read_by_pl_flag bpchar(1) NULL,
	cmt_created_by_supplier_flag bpchar(1) NULL,
	CONSTRAINT order_comment_pkey PRIMARY KEY (org_num_organization_ord, ord_order_number_ord, cmt_comment_id)
);
create user orders_as1_iso with password 'orders_as1_iso';
alter schema orders_as1_iso owner to orders_as1_iso;
alter database orders_as1_iso owner to orders_as1_iso;
alter table order_comment owner to orders_as1_iso;


create user orders_as1_iso identified by "orders_as1_iso";
alter user orders_as1_iso quota unlimited on system;
grant connect to orders_as1_iso;
CREATE TABLE "ORDERS_AS1_ISO"."ORDER_COMMENT" ( "ORG_NUM_ORGANIZATION_ORD" NUMBER(5), "ORD_ORDER_NUMBER_ORD" NUMBER(10), "CMT_COMMENT_ID" NUMBER(10), "CMT_COMMENT" VARCHAR2(255), "CMT_CREATION_DATE" TIMESTAMP(6), "CMT_UPDATE_DATE" TIMESTAMP(6), "CMT_UPDATER_PROFILE" VARCHAR2(255), "CMT_PRIVATE_FLAG" CHAR(1), "CMT_CREATION_IN_EXTRANET_FLAG" CHAR(1), "CMT_READ_BY_SUPPLIER_FLAG" CHAR(1), "CMT_READ_BY_PL_FLAG" CHAR(1), "CMT_CREATED_BY_SUPPLIER_FLAG" CHAR(1), CONSTRAINT "PK_ORDER_COMMENT" PRIMARY KEY ("ORG_NUM_ORGANIZATION_ORD", "ORD_ORDER_NUMBER_ORD", "CMT_COMMENT_ID") VALIDATE ) ;


update 