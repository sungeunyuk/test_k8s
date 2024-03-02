


create table anl_workspace.thomas_dataset_c360_tx_infer_target_5min stored as parquet as 
with MERC_TPBS as (
select *
from (
select b.merc_tpbs_dvcd
,b.merc_tpbs_nm
,row_number() over(partition by b.merc_tpbs_dvcd order by b.aply_dt desc, b.rec_seqno desc) as priority
from edw.dpvmr_vpcag_b b /*VMR_EDW업종별가맹점수수료율기준*/
약 43만건 
where 1=1
and b.aply_dt <= '20240213'
and b.merc_tpbs_cd != 'MCC' /*Merchant Category Code: 해외기준에 매핑되는 가맹점 업종코드*/
) as T
where priority = 1
)
, mncd as (
select cstno, count(*) as nrom_sta_mncd_ccnt /*정상상태미니카드건수*/
from cdp.chcr /*C360_체크카드*/
where iss_dt <= '20240213' /*카드발급일자*/
and card_stcd = '0001' /*카드상태코드(0001 정상 / 0008 취소 / 0009 해지)*/
and card_gds_alnc_cd = '0002' /*카드상품제휴코드: 0001 프렌즈 체크카드 / 0002 mini카드 / 0003 개인사업자 체크카드*/
group by 1
), cust as (
select cstno
, sex_cd
, cast(trunc(int_months_between(concat(substr('20240213', 1, 4), '-', substr('20240213', 5, 2), '-', substr('20240213', 7, 2))
,concat(substr(brdd,         1, 4), '-', substr(brdd,         5, 2), '-', substr(brdd,         7, 2))
) / 12
) as SMALLINT
) as crm_age
from cdp.customer /*C360_고객*/
)
select tx.tx_dt                                                                                         as tx_dt
, case when bzdd.holday_cd = '02' then 1 else 0 end                                                as tx_lgl_holday_yn
, bzdd.bzdd_yn                                                                                     as tx_bzdd_yn
, tx.tx_time                                                                                       as tx_time
, left(tx.tx_time,2)                                                                               as tx_time_hh
, case when left(TX.tx_time,2) between '00' and '06' then '00_06'
when left(TX.tx_time,2) between '07' and '11' then '07_11'
when left(TX.tx_time,2) between '12' and '18' then '12_18'
when left(TX.tx_time,2) between '19' and '23' then '19_23' end                              as tx_tmzn
, to_timestamp(concat(tx.tx_dt, rpad(tx.tx_time,8,'0')), 'yyyyMMddHHmmssSS')                       as tx_dttm
, tx.guid                                                                                          as guid
, mncd.nrom_sta_mncd_ccnt                                                                          as nrom_sta_mncd_ccnt
, tx.chcr_card_no                                                                                  as card_no
, chcr.card_gds_alnc_cd                                                                            as card_gds_alnc_cd
, chcr.frst_card_iss_dt                                                                            as frst_card_iss_dt
, chcr.iss_dt                                                                                      as card_iss_dt
, case when aprv_pos_tx_dvcd.rthn_card_ned_yn = 'Y' then 1
when aprv_pos_tx_dvcd.rthn_card_ned_yn = 'N' then 0
end                                                                                             as rthn_card_ned_yn
, tx.dep_tx_evnt_dvcd
, case when tx.dep_tx_evnt_dvcd in ('현금출금', '현금입금', 'ATM이체', '스마트출금') then 'OFFLINE'
when tx.dep_tx_evnt_dvcd = '체크카드결제' then aprv_pos_tx_dvcd.tx_div_nm
end                                                                                             as tx_div_nm
, aprv_pos_tx_dvcd.tx_mhod_nm                                                                      as tx_mhod_nm
, abs(tx.tx_amt)                                                                                   as tx_amt
, case when tx.dep_tx_evnt_dvcd = '체크카드결제'                                     then 1 else 0 end  as uplc_div_chcap_yn
, case when tx.dep_tx_evnt_dvcd in ('현금출금', '현금입금', 'ATM이체', '스마트출금')   then 1 else 0 end  as uplc_div_atmc_yn
, case when tx.dep_tx_evnt_dvcd in ('현금출금', '현금입금', '스마트출금')              then 1 else 0 end  as uplc_div_atmc_cashtx_yn
, case when tx.dep_tx_evnt_dvcd = '현금입금'                                         then 1 else 0 end  as uplc_div_atmc_mnrc_yn
, case when tx.dep_tx_evnt_dvcd in ('현금출금', '스마트출금')                         then 1 else 0 end  as uplc_div_atmc_wdl_yn
, case when tx.dep_tx_evnt_dvcd = 'ATM이체'                                          then 1 else 0 end  as uplc_div_atmc_trsf_yn
, upper(nvl(regexp_replace(tx.chcr_merc_nm,'[\u3000\\s]+',' '), concat_ws('|', addr_cdn_atmc.bank_nm, addr_cdn_atmc.brof_nm))) as uplc_nm
, case when tx.dep_tx_evnt_dvcd = '체크카드결제'             then 'chcap'
when tx.dep_tx_evnt_dvcd = '현금입금'                 then 'atmc_mnrc'
when tx.dep_tx_evnt_dvcd in ('현금출금', '스마트출금') then 'atmc_wdl'
when tx.dep_tx_evnt_dvcd = 'ATM이체'                  then 'atmc_trsf'
end                                                                                             as uplc_div_nm
, case when tx.dep_tx_evnt_dvcd = '체크카드결제' then nvl(regexp_replace(MERC_TPBS.merc_tpbs_nm,'[\u3000\\s]+',''), 'Unknown')
when tx.dep_tx_evnt_dvcd = '현금입금'                 then 'ATM입금'
when tx.dep_tx_evnt_dvcd in ('현금출금', '스마트출금') then 'ATM출금'
when tx.dep_tx_evnt_dvcd = 'ATM이체'                  then 'ATM이체'
else 'Unknown'
end                                                                                             as uplc_clss_nm
, case when tx.dep_tx_evnt_dvcd in ('현금출금', '현금입금', 'ATM이체', '스마트출금') then 1
when aprv_pos_tx_dvcd.tx_div_nm = 'OFFLINE' then 1
else 0
end                                                                                             as uplc_ofln_yn
, addr_cdn_onhm.x_cdn_utm_val     as addr_cdn_onhm_x
, addr_cdn_onhm.y_cdn_utm_val     as addr_cdn_onhm_y
, addr_cdn_wrst.x_cdn_utm_val     as addr_cdn_wrst_x
, addr_cdn_wrst.y_cdn_utm_val     as addr_cdn_wrst_y
, nvl(addr_cdn_merc.x_cdn_utm_val,     addr_cdn_atmc.x_cdn_utm_val)     as addr_cdn_uplc_x
, nvl(addr_cdn_merc.y_cdn_utm_val,     addr_cdn_atmc.y_cdn_utm_val)     as addr_cdn_uplc_y
, sqrt(pow(nvl(addr_cdn_merc.x_cdn_utm_val, addr_cdn_atmc.x_cdn_utm_val) - addr_cdn_onhm.x_cdn_utm_val, 2) + pow(nvl(addr_cdn_merc.y_cdn_utm_val, addr_cdn_atmc.y_cdn_utm_val) - addr_cdn_onhm.y_cdn_utm_val, 2))/1000 as dstn_bw_onhm_km
, sqrt(pow(nvl(addr_cdn_merc.x_cdn_utm_val, addr_cdn_atmc.x_cdn_utm_val) - addr_cdn_wrst.x_cdn_utm_val, 2) + pow(nvl(addr_cdn_merc.y_cdn_utm_val, addr_cdn_atmc.y_cdn_utm_val) - addr_cdn_wrst.y_cdn_utm_val, 2))/1000 as dstn_bw_wrst_km
, cust.sex_cd                                                                                      as cst_sex_cd
, cust.crm_age                                                                                     as cst_age
, case when cust.crm_age is null then null
when cust.crm_age <= 9  then 0
when cust.crm_age >= 60 then 6
else cast(left(cast(cust.crm_age as string),1) as tinyint)
end                                                                                             as cst_argn
, idvd_cust_info.cdd_ocup_dvcd                                                                     as cdd_ocup_dvcd
, tx.cstno                                                                                         as cstno
from cdp.dep_tx_l as tx /*C360_수신거래내역*/
left join mncd
on (tx.cstno = mncd.cstno)
left join cust
on (tx.cstno = cust.cstno)
left join cdp.chcr /*C360_체크카드*/
on (tx.chcr_card_no = chcr.card_no)
left join cdp.idvd_cust_info /*C360_개인고객정보*/
on (tx.cstno = idvd_cust_info.cstno)
left join crm.uccrm_bzdd_mgmt_m as bzdd /*CRM영업일관리기본 -> base_dt='20170325' 4건 중복 주의*/
on (bzdd.base_dt = '20240213')
left join MERC_TPBS
on (tx.chcr_merc_tpbs_dvcd = MERC_TPBS.merc_tpbs_dvcd)
/*위경도 - 거주지*/
left join edw.dopcu_cuspa_p as cuspa_onhm
on (from_timestamp(date_sub(to_timestamp('20240213', 'yyyyMMdd'),2),'yyyyMMdd') = cuspa_onhm.base_dt
and tx.cstno = cuspa_onhm.cstno
and '001'    = cuspa_onhm.cust_cnpl_tycd) /* 고객연락처유형코드: 001 자택/사업장 / 002 직장 / 003 추가연락처1 / 004 잔액증명발급발송주소 / 005 카드연락처 / 009 비거주자거주지주소 / 098 사후실사연락처정보 */
left join serv.loct_cdn_cnpl_ref_r as addr_cdn_onhm
on (murmur_hash(concat_ws('|', nvl(cuspa_onhm.bldg_mgmt_no,'*')
, nvl(cuspa_onhm.addr_road_nm_mgmt_cd,'*')
, nvl(cuspa_onhm.road_nm_addr_ref_no,'*')
, nvl(cuspa_onhm.zadr,'*')
, nvl(cuspa_onhm.zpcd,'*'))) = addr_cdn_onhm.murmur_hash_key)
/*위경도 - 직장*/
left join edw.dopcu_cuspa_p as cuspa_wrst
on (from_timestamp(date_sub(to_timestamp('20240213', 'yyyyMMdd'),2),'yyyyMMdd') = cuspa_wrst.base_dt
and tx.cstno = cuspa_wrst.cstno
and '002'    = cuspa_wrst.cust_cnpl_tycd) /* 고객연락처유형코드: 001 자택/사업장 / 002 직장 / 003 추가연락처1 / 004 잔액증명발급발송주소 / 005 카드연락처 / 009 비거주자거주지주소 / 098 사후실사연락처정보 */
left join serv.loct_cdn_cnpl_ref_r as addr_cdn_wrst
on (murmur_hash(concat_ws('|', nvl(cuspa_wrst.bldg_mgmt_no,'*')
, nvl(cuspa_wrst.addr_road_nm_mgmt_cd,'*')
, nvl(cuspa_wrst.road_nm_addr_ref_no,'*')
, nvl(cuspa_wrst.zadr,'*')
, nvl(cuspa_wrst.zpcd,'*'))) = addr_cdn_wrst.murmur_hash_key)
/*위경도 - 카드가맹점*/
left join (select * from serv.loct_cdn_card_merc_p where base_dt = (select max(base_dt) from serv.loct_cdn_card_merc_p)) as addr_cdn_merc /*주소_좌표_카드가맹점*/
on (tx.dep_tx_evnt_dvcd = '체크카드결제'
and tx.chcr_merc_no     = addr_cdn_merc.bc_alnc_merc_no)
/*위경도 - 자동화기기*/
left join (select * from serv.loct_cdn_bank_gircd_p where base_dt = (select max(base_dt) from serv.loct_cdn_bank_gircd_p)) as addr_cdn_atmc /*주소_좌표_자동화기기*/
on (tx.dep_tx_evnt_dvcd in ('현금출금', '현금입금', 'ATM이체', '스마트출금')
and concat_ws('|', decode(tx.atmcash_ats_yn, 'Y', '2', '1'), tx.atmcash_hndl_bank_gircd) = concat_ws('|', addr_cdn_atmc.atm_netw_dvcd, addr_cdn_atmc.bank_gircd))
left join serv.card_aprv_pos_tx_dvcd_c as aprv_pos_tx_dvcd /*승인POS거래구분코드 - 매핑테이블*/
on (tx.chcr_aprv_pos_tx_dvcd = aprv_pos_tx_dvcd.cd_val)
where 1=1
and tx.tx_dt between '20240213' and '20240213'
and to_timestamp(concat(tx.tx_dt, rpad(tx.tx_time,12,'0')), 'yyyyMMddHHmmssSSSSSS') >  to_timestamp('202402XXXXX000', 'yyyyMMddHHmmssSSSSSS')
and to_timestamp(concat(tx.tx_dt, rpad(tx.tx_time,12,'0')), 'yyyyMMddHHmmssSSSSSS') <= to_timestamp('202402XXXXX623',     'yyyyMMddHHmmssSSSSSS')
/*filter-out by needs*/
and abs(tx.tx_amt) >= 5000 and dep_tx_evnt_dvcd in ('체크카드결제', '현금출금', '스마트출금')
and nvl(tx.chcr_merc_cntry_cd,'') in ('', 'KR')
;

create table thomas_dataset_tx_dmsc_prep_frd_card_esti_5min stored as parquet as
with MERC_TPBS as (
select *
from (
select b.merc_tpbs_dvcd
,b.merc_tpbs_nm
,row_number() over(partition by b.merc_tpbs_dvcd order by b.aply_dt desc, b.rec_seqno desc) as priority
from edw.dpvmr_vpcag_b b /*VMR_EDW업종별가맹점수수료율기준*/
where 1=1
and b.aply_dt <= '20240213'
and b.merc_tpbs_cd != 'MCC' /*Merchant Category Code: 해외기준에 매핑되는 가맹점 업종코드*/
) as T
where priority = 1
)
, tx_before_90d as (
select tx.guid
, tx.uplc_nm
, tx.uplc_clss_nm

, tx_before.guid as guid_before
, (unix_timestamp(tx.tx_dttm) - unix_timestamp(to_timestamp(concat(tx_before.tx_dt, rpad(tx_before.tx_time,8,'0')), 'yyyyMMddHHmmssSS'))) as tx_time_diff_rbf
, upper(nvl(regexp_replace(tx_before.chcr_merc_nm,'[\u3000\\s]+',' '), concat_ws('|', addr_cdn_atmc.bank_nm, addr_cdn_atmc.brof_nm))) as uplc_nm_before
, nvl(regexp_replace(MERC_TPBS.merc_tpbs_nm,'[\u3000\\s]+',''), 'Unknown') as uplc_clss_nm_before

, tx_before.tx_dt as tx_dt_before
from      anl_workspace.thomas_dataset_c360_tx_infer_target_5min as tx /*C360_수신거래내역 -> inference target 추출*/
inner join cdp.dep_tx_l as tx_before /*C360_수신거래내역*/
on (tx.cstno = tx_before.cstno
and tx_before.dep_tx_evnt_dvcd in ('체크카드결제', '현금출금', '현금입금', 'ATM이체', '스마트출금')
/*before는 90일전까지만*/
and tx_before.tx_dt between from_timestamp(date_sub(to_timestamp('20240213', 'yyyyMMdd'),90),'yyyyMMdd') and '20240213'
/* 1초 전부터 ~ */
and (unix_timestamp(tx.tx_dttm) - unix_timestamp(to_timestamp(concat(tx_before.tx_dt, rpad(tx_before.tx_time,8,'0')), 'yyyyMMddHHmmssSS'))) >= 1)
left join MERC_TPBS
on (tx_before.chcr_merc_tpbs_dvcd = MERC_TPBS.merc_tpbs_dvcd)
/*위경도 - 자동화기기*/
left join (select * from serv.loct_cdn_bank_gircd_p where base_dt = (select max(base_dt) from serv.loct_cdn_bank_gircd_p)) as addr_cdn_atmc /*주소_좌표_자동화기기*/
on (tx_before.dep_tx_evnt_dvcd in ('현금출금', '현금입금', 'ATM이체', '스마트출금')
and concat_ws('|', decode(tx_before.atmcash_ats_yn, 'Y', '2', '1'), tx_before.atmcash_hndl_bank_gircd) = concat_ws('|', addr_cdn_atmc.atm_netw_dvcd, addr_cdn_atmc.bank_gircd))
)
, cst_tx_90d_uplc_clss as (
select tx.guid
, min(tx.tx_time_diff_rbf)/60/60/24 as tx_time_diff_rbf
from tx_before_90d as tx
/*동일한 사용처분류*/
where tx.uplc_clss_nm = tx.uplc_clss_nm_before
group by 1
)
, cst_tx_90d_uplc as (
select tx.guid
, min(tx.tx_time_diff_rbf)/60/60/24 as tx_time_diff_rbf
from tx_before_90d as tx
/*동일한 사용처*/
where tx.uplc_nm = tx.uplc_nm_before
group by 1
)
, cst_tx_90d as (
select tx.guid
, min(tx.tx_time_diff_rbf)/60/60/24 as tx_time_diff_rbf
from tx_before_90d as tx
group by 1
)
, tx_before_24h as (
select tx.guid
, tx.cstno
, tx.uplc_clss_nm
, tx.uplc_ofln_yn
, tx.addr_cdn_uplc_x
, tx.addr_cdn_uplc_y
, tx.addr_cdn_onhm_x
, tx.addr_cdn_onhm_y
, tx.addr_cdn_wrst_x
, tx.addr_cdn_wrst_y

, tx_before.guid as guid_before
, tx_before.dep_tx_evnt_dvcd as dep_tx_evnt_dvcd_before
, tx_before.tx_amt as tx_amt_before
, to_timestamp(concat(tx_before.tx_dt, rpad(tx_before.tx_time,8,'0')), 'yyyyMMddHHmmssSS') as tx_dttm_before
, (unix_timestamp(tx.tx_dttm) - unix_timestamp(to_timestamp(concat(tx_before.tx_dt, rpad(tx_before.tx_time,8,'0')), 'yyyyMMddHHmmssSS'))) as tx_time_diff_rbf
, nvl(regexp_replace(MERC_TPBS.merc_tpbs_nm,'[　\s]+',''), 'Unknown') as uplc_clss_nm_before
, case when tx_before.dep_tx_evnt_dvcd in ('현금출금', '현금입금', 'ATM이체', '스마트출금') then 1
when aprv_pos_tx_dvcd.tx_div_nm = 'OFFLINE' then 1
else 0
end as uplc_ofln_yn_before
, tx_before.chcr_merc_cntry_cd as chcr_merc_cntry_cd_before
, nvl(addr_cdn_merc.x_cdn_utm_val,     addr_cdn_atmc.x_cdn_utm_val)     as addr_cdn_uplc_x_before
, nvl(addr_cdn_merc.y_cdn_utm_val,     addr_cdn_atmc.y_cdn_utm_val)     as addr_cdn_uplc_y_before
from      anl_workspace.thomas_dataset_c360_tx_infer_target_5min as tx /*C360_수신거래내역 -> inference target 추출*/
inner join cdp.dep_tx_l as tx_before /*C360_수신거래내역*/
on (tx.cstno = tx_before.cstno
and tx_before.dep_tx_evnt_dvcd in ('체크카드결제', '현금출금', '현금입금', 'ATM이체', '스마트출금')
/*before는 하루전까지만*/
and tx_before.tx_dt between from_timestamp(date_sub(to_timestamp('20230926', 'yyyyMMdd'),1),'yyyyMMdd') and '20230926'
/*24시간 이내*/
and (unix_timestamp(tx.tx_dttm) - unix_timestamp(to_timestamp(concat(tx_before.tx_dt, rpad(tx_before.tx_time,8,'0')), 'yyyyMMddHHmmssSS'))) between 1 and 24*60*60)
left join MERC_TPBS
on (tx_before.chcr_merc_tpbs_dvcd = MERC_TPBS.merc_tpbs_dvcd)
/*위경도 - 카드가맹점*/
left join (select * from serv.loct_cdn_card_merc_p where base_dt = (select max(base_dt) from serv.loct_cdn_card_merc_p)) as addr_cdn_merc /*주소_좌표_카드가맹점*/
on (tx_before.dep_tx_evnt_dvcd = '체크카드결제'
and tx_before.chcr_merc_no     = addr_cdn_merc.bc_alnc_merc_no)
/*위경도 - 자동화기기*/
left join (select * from serv.loct_cdn_bank_gircd_p where base_dt = (select max(base_dt) from serv.loct_cdn_bank_gircd_p)) as addr_cdn_atmc /*주소_좌표_자동화기기*/
on (tx_before.dep_tx_evnt_dvcd in ('체크카드결제', '현금출금', '현금입금', 'ATM이체', '스마트출금')
and concat_ws('|', decode(tx_before.atmcash_ats_yn, 'Y', '2', '1'), tx_before.atmcash_hndl_bank_gircd) = concat_ws('|', addr_cdn_atmc.atm_netw_dvcd, addr_cdn_atmc.bank_gircd))
left join serv.card_aprv_pos_tx_dvcd_c as aprv_pos_tx_dvcd /*승인POS거래구분코드 - 매핑테이블*/
on (tx_before.chcr_aprv_pos_tx_dvcd = aprv_pos_tx_dvcd.cd_val)
)
, cst_tx_24h as (
select tx.guid
, count(*) as cnt, sum(tx.tx_amt_before) as tx_amt, sum(tx.tx_amt_before)/count(*) as tx_unit_amt
from tx_before_24h as tx
group by 1
)
, cst_tx_24h_ofln as (
select tx.guid
, min(tx_time_diff_rbf)/60/60/24 as tx_time_diff_rbf
, max(tx_dttm_before)            as tx_dttm_rbf
, max((sqrt(pow(tx.addr_cdn_uplc_x_before - tx.addr_cdn_uplc_x, 2) + pow(tx.addr_cdn_uplc_y_before - tx.addr_cdn_uplc_y, 2))/1000))                             as dstn_bw_rbf_km
, max((sqrt(pow(tx.addr_cdn_uplc_x_before - tx.addr_cdn_uplc_x, 2) + pow(tx.addr_cdn_uplc_y_before - tx.addr_cdn_uplc_y, 2))/1000)/(tx.tx_time_diff_rbf/60/60)) as sped_bw_rbf_kmh
from tx_before_24h as tx
where 1=1
/*사용처 주소 수집 가능대상*/
and tx.addr_cdn_uplc_x is not null
and tx.uplc_ofln_yn = 1
and tx.uplc_clss_nm not in ('택시', '고속.시외버스', '화물운송업', '기타운송수단', '철도', 'RF유료도로.터널')
and tx.uplc_clss_nm not like '%전자상거래%'
/*사용처 주소 수집 가능대상*/
and tx.addr_cdn_uplc_x_before is not null
and tx.uplc_ofln_yn_before = 1
and tx.uplc_clss_nm_before not in ('택시', '고속.시외버스', '화물운송업', '기타운송수단', '철도', 'RF유료도로.터널')
and tx.uplc_clss_nm_before not like '%전자상거래%'
group by 1
)
, cst_tx_24h_abrd as (
select tx.guid
, count(*) as cnt, sum(tx.tx_amt_before) as tx_amt, sum(tx.tx_amt_before)/count(*) as tx_unit_amt
from tx_before_24h as tx
where 1=1
and tx.chcr_merc_cntry_cd_before != 'KR' and length(tx.chcr_merc_cntry_cd_before)>=2
and tx.uplc_ofln_yn_before = 1
group by 1
)
, cst_tx_24h_recv as (
select tx.guid
/*당타발구분코드: 1 당발 / 2 타발*/ /*본인명의: 의뢰인명 = 수취인명*/ /*출금계좌길이유효*/
, sum(case when (tx_before.whbn_cd !='090' or regexp_replace(upper(customer.cust_nm),'[^가-힣A-Z]','') != regexp_replace(upper(tx_before.recv_nm),'[^가-힣A-Z]','')) and length(nvl(tx_before.wdl_acno,''))>=1 then 1 end)                as tx_24h_obnk_otpp_ojbb_acno_mnrc_cnt /*최근24시간 타행/타인->당행계좌 입금 건수*/
, sum(case when (tx_before.whbn_cd !='090' or regexp_replace(upper(customer.cust_nm),'[^가-힣A-Z]','') != regexp_replace(upper(tx_before.recv_nm),'[^가-힣A-Z]','')) and length(nvl(tx_before.wdl_acno,''))>=1 then tx_before.tx_amt end) as tx_24h_obnk_otpp_ojbb_acno_mnrc_amt /*최근24시간 타행/타인->당행계좌 입금 총액*/
, min(case when (tx_before.whbn_cd !='090' or regexp_replace(upper(customer.cust_nm),'[^가-힣A-Z]','') != regexp_replace(upper(tx_before.recv_nm),'[^가-힣A-Z]','')) and length(nvl(tx_before.wdl_acno,''))>=1
then unix_timestamp(tx.tx_dttm) - unix_timestamp(to_timestamp(concat(tx_before.tx_dt, rpad(tx_before.tx_time,8,'0')), 'yyyyMMddHHmmssSS')) end)/60/60/24 as tx_24h_obnk_otpp_ojbb_acno_mnrc_time_diff /*최근24시간 타행/타인->당행계좌 입금으로부터 지난 시간*/
, max(case when (tx_before.whbn_cd !='090' or regexp_replace(upper(customer.cust_nm),'[^가-힣A-Z]','') != regexp_replace(upper(tx_before.recv_nm),'[^가-힣A-Z]','')) and length(nvl(tx_before.wdl_acno,''))>=1
then sqrt(pow(addr_cdn_atmc.x_cdn_utm_val - tx.addr_cdn_onhm_x,            2) + pow(addr_cdn_atmc.y_cdn_utm_val - tx.addr_cdn_onhm_y,            2))/1000 end) as tx_24h_obnk_otpp_ojbb_acno_mnrc_dstn_bw_onhm_km /*최근24시간 타행/타인->당행계좌 입금된 계좌관리점과 거주지간 거리 (km)*/
, max(case when (tx_before.whbn_cd !='090' or regexp_replace(upper(customer.cust_nm),'[^가-힣A-Z]','') != regexp_replace(upper(tx_before.recv_nm),'[^가-힣A-Z]','')) and length(nvl(tx_before.wdl_acno,''))>=1
then sqrt(pow(addr_cdn_atmc.x_cdn_utm_val - tx.addr_cdn_wrst_x,            2) + pow(addr_cdn_atmc.y_cdn_utm_val - tx.addr_cdn_wrst_y,            2))/1000 end) as tx_24h_obnk_otpp_ojbb_acno_mnrc_dstn_bw_wrst_km /*최근24시간 타행/타인->당행계좌 입금된 계좌관리점과 직장간 거리 (km)*/
, max(case when (tx_before.whbn_cd !='090' or regexp_replace(upper(customer.cust_nm),'[^가-힣A-Z]','') != regexp_replace(upper(tx_before.recv_nm),'[^가-힣A-Z]','')) and length(nvl(tx_before.wdl_acno,''))>=1
then sqrt(pow(addr_cdn_atmc.x_cdn_utm_val - prep_dstn.addr_cdn_uplc_x_avg, 2) + pow(addr_cdn_atmc.y_cdn_utm_val - prep_dstn.addr_cdn_uplc_y_avg, 2))/1000 end) as tx_24h_obnk_otpp_ojbb_acno_mnrc_dstn_bw_uplc_avg_km /*최근24시간 타행/타인->당행계좌 입금된 계좌관리점과 거래중심지간 거리 (km)*/
, max(case when (tx_before.whbn_cd !='090' or regexp_replace(upper(customer.cust_nm),'[^가-힣A-Z]','') != regexp_replace(upper(tx_before.recv_nm),'[^가-힣A-Z]','')) and length(nvl(tx_before.wdl_acno,''))>=1
then sqrt(pow(addr_cdn_atmc.x_cdn_utm_val - tx.addr_cdn_uplc_x,            2) + pow(addr_cdn_atmc.y_cdn_utm_val - tx.addr_cdn_uplc_y,            2))/1000 end) as tx_24h_obnk_otpp_ojbb_acno_mnrc_dstn_bw_uplc_km /*최근24시간 타행/타인->당행계좌 입금된 계좌관리점과 사용처간 거리 (km)*/

, sum(case when tx_before.dep_tx_evnt_dvcd = '현금입금'
then 1 end)                as tx_24h_atmc_ojbb_acno_mnrc_cnt /*최근24시간 자동화기기-> 당행계좌 입금 건수*/
, sum(case when tx_before.dep_tx_evnt_dvcd = '현금입금'
then tx_before.tx_amt end) as tx_24h_atmc_ojbb_acno_mnrc_amt /*최근24시간 자동화기기-> 당행계좌 입금 총액*/
, min(case when tx_before.dep_tx_evnt_dvcd = '현금입금'
then unix_timestamp(tx.tx_dttm) - unix_timestamp(to_timestamp(concat(tx_before.tx_dt, rpad(tx_before.tx_time,8,'0')), 'yyyyMMddHHmmssSS')) end)/60/60/24 as tx_24h_atmc_ojbb_acno_mnrc_time_diff /*최근24시간 자동화기기->당행계좌 입금으로부터 지난 시간*/
, max(case when tx_before.dep_tx_evnt_dvcd = '현금입금'
then sqrt(pow(addr_cdn_atmc.x_cdn_utm_val - tx.addr_cdn_onhm_x,            2) + pow(addr_cdn_atmc.y_cdn_utm_val - tx.addr_cdn_onhm_y,            2))/1000 end) as tx_24h_atmc_ojbb_acno_mnrc_dstn_bw_onhm_km /*최근24시간 자동화기기->당행계좌 입금된 계좌관리점과 거주지간 거리 (km)*/
, max(case when tx_before.dep_tx_evnt_dvcd = '현금입금'
then sqrt(pow(addr_cdn_atmc.x_cdn_utm_val - tx.addr_cdn_wrst_x,            2) + pow(addr_cdn_atmc.y_cdn_utm_val - tx.addr_cdn_wrst_y,            2))/1000 end) as tx_24h_atmc_ojbb_acno_mnrc_dstn_bw_wrst_km /*최근24시간 자동화기기->당행계좌 입금된 계좌관리점과 직장간 거리 (km)*/
, max(case when tx_before.dep_tx_evnt_dvcd = '현금입금'
then sqrt(pow(addr_cdn_atmc.x_cdn_utm_val - prep_dstn.addr_cdn_uplc_x_avg, 2) + pow(addr_cdn_atmc.y_cdn_utm_val - prep_dstn.addr_cdn_uplc_y_avg, 2))/1000 end) as tx_24h_atmc_ojbb_acno_mnrc_dstn_bw_uplc_avg_km /*최근24시간 자동화기기->당행계좌 입금된 계좌관리점과 거래중심지간 거리 (km)*/
, max(case when tx_before.dep_tx_evnt_dvcd = '현금입금'
then sqrt(pow(addr_cdn_atmc.x_cdn_utm_val - tx.addr_cdn_uplc_x,            2) + pow(addr_cdn_atmc.y_cdn_utm_val - tx.addr_cdn_uplc_y,            2))/1000 end) as tx_24h_atmc_ojbb_acno_mnrc_dstn_bw_uplc_km /*최근24시간 자동화기기->당행계좌 입금된 계좌관리점과 사용처간 거리 (km)*/
from      anl_workspace.thomas_dataset_c360_tx_infer_target_5min as tx /*C360_수신거래내역 -> inference target 추출*/
inner join cdp.dep_tx_l                                      as tx_before /*C360_수신거래내역*/
on (tx.cstno = tx_before.cstno
and tx_before.dep_tx_evnt_dvcd in ('간편이체', '오픈뱅킹이체', '일반이체', '현금입금', '현금출금', '스마트출금', 'ATM이체')
/*before는 하루전까지만*/
and tx_before.tx_dt between from_timestamp(date_sub(to_timestamp('20240213', 'yyyyMMdd'),1),'yyyyMMdd') and '20240213'
/*24시간 이내*/
and (unix_timestamp(tx.tx_dttm) - unix_timestamp(to_timestamp(concat(tx_before.tx_dt, rpad(tx_before.tx_time,8,'0')), 'yyyyMMddHHmmssSS'))) between 1 and 24*60*60
/*수취은행코드: 090 카카오뱅크*/
and tx_before.rcbk_cd = '090')
inner join cdp.customer /*C360_고객*/
on (tx_before.cstno = customer.cstno)
left join (select * from serv.loct_cdn_bank_gircd_p where base_dt = (select max(base_dt) from serv.loct_cdn_bank_gircd_p)) as addr_cdn_atmc /*주소_좌표_자동화기기*/
on (tx_before.dep_tx_evnt_dvcd in ('현금출금', '현금입금', 'ATM이체', '스마트출금')
and concat_ws('|', decode(tx_before.atmcash_ats_yn, 'Y', '2', '1'), tx_before.atmcash_hndl_bank_gircd) = concat_ws('|', addr_cdn_atmc.atm_netw_dvcd, addr_cdn_atmc.bank_gircd))
left join anl_workspace.thomas_dataset_tx_dmsc_prep_3m_cst_addr_cdn as prep_dstn
on (from_timestamp(date_sub(to_timestamp('20240213', 'yyyyMMdd'),2),'yyyyMMdd') = prep_dstn.base_dt
and tx.cstno = prep_dstn.cstno)
group by 1
)
, cst_tx_24h_atmc as (
select tx.guid
/*자동화기기거래구분코드: 출금, 스마트출금, QR스마트출금*/ /*대외채널전문상태코드 : (02)정상완료,(05)취소완료,(06)에러응답,(07)불능응답,(15)부분환불완료*/
, sum(case when tx.dep_tx_evnt_dvcd_before in ('현금출금', '스마트출금') and tx.tx_amt_before <= 10000   then 1 end) as tx_24h_atmc_wdl_tw10_cnt     /*최근24시간 자동화기기 1만원 이하 출금 횟수*/
, min(case when tx.dep_tx_evnt_dvcd_before in ('현금출금', '스마트출금') and tx.tx_amt_before <= 10000
then tx.tx_time_diff_rbf end)/60/60/24 as tx_24h_atmc_wdl_tw10_time_diff /*최근24시간 자동화기기 1만원 이하 출금으로부터 지난 시간*/
, max(case when tx.dep_tx_evnt_dvcd_before in ('현금출금', '스마트출금') and tx.tx_amt_before <= 10000
then sqrt(pow(tx.addr_cdn_uplc_x_before - tx.addr_cdn_onhm_x,            2) + pow(tx.addr_cdn_uplc_y_before - tx.addr_cdn_onhm_y,            2))/1000 end) as tx_24h_atmc_wdl_tw10_dstn_bw_onhm_km /*최근24시간 자동화기기 1만원 이하 출금 지점과 거주지간 거리 (km)*/
, max(case when tx.dep_tx_evnt_dvcd_before in ('현금출금', '스마트출금') and tx.tx_amt_before <= 10000
then sqrt(pow(tx.addr_cdn_uplc_x_before - tx.addr_cdn_wrst_x,            2) + pow(tx.addr_cdn_uplc_y_before - tx.addr_cdn_wrst_y,            2))/1000 end) as tx_24h_atmc_wdl_tw10_dstn_bw_wrst_km /*최근24시간 자동화기기 1만원 이하 출금 지점과 직장간 거리 (km)*/
, max(case when tx.dep_tx_evnt_dvcd_before in ('현금출금', '스마트출금') and tx.tx_amt_before <= 10000
then sqrt(pow(tx.addr_cdn_uplc_x_before - prep_dstn.addr_cdn_uplc_x_avg, 2) + pow(tx.addr_cdn_uplc_y_before - prep_dstn.addr_cdn_uplc_y_avg, 2))/1000 end) as tx_24h_atmc_wdl_tw10_dstn_bw_uplc_avg_km /*최근24시간 자동화기기 1만원 이하 출금 지점과 거래중심지간 거리 (km)*/
, max(case when tx.dep_tx_evnt_dvcd_before in ('현금출금', '스마트출금') and tx.tx_amt_before <= 10000
then sqrt(pow(tx.addr_cdn_uplc_x_before - tx.addr_cdn_uplc_x,            2) + pow(tx.addr_cdn_uplc_y_before - tx.addr_cdn_uplc_y,            2))/1000 end) as tx_24h_atmc_wdl_tw10_dstn_bw_uplc_km /*최근24시간 자동화기기 1만원 이하 출금 지점과 사용처간 거리 (km)*/

from tx_before_24h as tx
inner join cdp.dep_acco as acno_cstno /*C360_수신계좌*/
on (tx.cstno = acno_cstno.cstno)
left join anl_workspace.thomas_dataset_tx_dmsc_prep_3m_cst_addr_cdn as prep_dstn
on (from_timestamp(date_sub(to_timestamp('20240213', 'yyyyMMdd'),2),'yyyyMMdd') = prep_dstn.base_dt
and tx.cstno = prep_dstn.cstno)
where 1=1
and tx.dep_tx_evnt_dvcd_before in ('현금입금', '현금출금', '스마트출금', 'ATM이체')
group by 1
)
, cst_tx_24h_lnbz as (
select tx.guid
, min(case when substr(tx_before.gds_cd,6,4) in ('3001','3023')                             then unix_timestamp(tx.tx_dttm) - unix_timestamp(to_timestamp(rpad(tx_before.reg_dttm,17,'0'), 'yyyyMMddHHmmssSSS')) end)/60/60/24 as lnbz_exec_time_diff_nstg /*비상금*/
, min(case when substr(tx_before.gds_cd,6,4) in ('3013','3015','3002','3024','3008','3014') then unix_timestamp(tx.tx_dttm) - unix_timestamp(to_timestamp(rpad(tx_before.reg_dttm,17,'0'), 'yyyyMMddHHmmssSSS')) end)/60/60/24 as lnbz_exec_time_diff_cred /*신용*/
, min(case when substr(tx_before.gds_cd,6,4) in ('3004','3020')                             then unix_timestamp(tx.tx_dttm) - unix_timestamp(to_timestamp(rpad(tx_before.reg_dttm,17,'0'), 'yyyyMMddHHmmssSSS')) end)/60/60/24 as lnbz_exec_time_diff_grmn /*보증금*/
, min(case when substr(tx_before.gds_cd,6,4) in ('3005')                                    then unix_timestamp(tx.tx_dttm) - unix_timestamp(to_timestamp(rpad(tx_before.reg_dttm,17,'0'), 'yyyyMMddHHmmssSSS')) end)/60/60/24 as lnbz_exec_time_diff_rles /*부동산담보*/
, min(case when substr(tx_before.gds_cd,6,4) in ('1001','3017')                             then unix_timestamp(tx.tx_dttm) - unix_timestamp(to_timestamp(rpad(tx_before.reg_dttm,17,'0'), 'yyyyMMddHHmmssSSS')) end)/60/60/24 as lnbz_exec_time_diff_busi /*사업자*/
, min(case when substr(tx_before.gds_cd,6,4) in ('3021','3022')                             then unix_timestamp(tx.tx_dttm) - unix_timestamp(to_timestamp(rpad(tx_before.reg_dttm,17,'0'), 'yyyyMMddHHmmssSSS')) end)/60/60/24 as lnbz_exec_time_diff_sls  /*햇살론*/
, sum(case when substr(tx_before.gds_cd,6,4) in ('3001','3023')                             then nvl(tx_before.loan_exec_amt,tx_before.mnus_limt_amt) else 0 end) as lnbz_exec_amt_nstg /*비상금*/
, sum(case when substr(tx_before.gds_cd,6,4) in ('3013','3015','3002','3024','3008','3014') then nvl(tx_before.loan_exec_amt,tx_before.mnus_limt_amt) else 0 end) as lnbz_exec_amt_cred /*신용*/
, sum(case when substr(tx_before.gds_cd,6,4) in ('3004','3020')                             then nvl(tx_before.loan_exec_amt,tx_before.mnus_limt_amt) else 0 end) as lnbz_exec_amt_grmn /*보증금*/
, sum(case when substr(tx_before.gds_cd,6,4) in ('3005')                                    then nvl(tx_before.loan_exec_amt,tx_before.mnus_limt_amt) else 0 end) as lnbz_exec_amt_rles /*부동산담보*/
, sum(case when substr(tx_before.gds_cd,6,4) in ('1001','3017')                             then nvl(tx_before.loan_exec_amt,tx_before.mnus_limt_amt) else 0 end) as lnbz_exec_amt_busi /*사업자*/
, sum(case when substr(tx_before.gds_cd,6,4) in ('3021','3022')                             then nvl(tx_before.loan_exec_amt,tx_before.mnus_limt_amt) else 0 end) as lnbz_exec_amt_sls  /*햇살론*/
from      anl_workspace.thomas_dataset_c360_tx_infer_target_5min as tx /*C360_수신거래내역 -> inference target 추출*/
inner join cdp.lnbz_acco                                     as tx_before /*C360_여신계좌*/
on (tx.cstno = tx_before.cstno
/*before는 하루전까지만*/
and tx_before.exec_dt between from_timestamp(date_sub(to_timestamp('20240213', 'yyyyMMdd'),1),'yyyyMMdd') and '20240213'
/*24시간 이내*/
and (unix_timestamp(tx.tx_dttm) - unix_timestamp(to_timestamp(rpad(tx_before.reg_dttm,17,'0'), 'yyyyMMddHHmmssSSS')) between 1 and 24*60*60)
/*여신신규구분코드(00 해당없음 01 신규 02 재약정 03 대환 06 한도증액 11 기한연장 12 채무인수)*/
and nvl(tx_before.lnbz_new_dvcd,'00') in ('00','01','06'))
/*실행금액 존재*/
and nvl(tx_before.loan_exec_amt,0) + nvl(tx_before.mnus_limt_amt,0) > 0
group by 1
)
, chcr_limt_chng as (
select tx.guid
, min(unix_timestamp(tx.tx_dttm) - unix_timestamp(to_timestamp(rpad(tx_before.chng_dttm,17,'0'), 'yyyyMMddHHmmssSSS')))/60/60/24 as chcr_limt_chng_time_diff
, count(*)                                                                                                                       as chcr_limt_chng_cnt
from      cdp.chcr_limt_chng_h as tx_before /*C360_체크카드한도변경이력 - 변경일시, 일한도금액, 월한도금액*/
inner join cdp.chcr as chcr /*C360_체크카드*/
on (tx_before.memb_no = chcr.memb_no)
inner join anl_workspace.thomas_dataset_c360_tx_infer_target_5min as tx /*C360_수신거래내역 -> inference target 추출*/
on (chcr.cstno = tx.cstno
/*before는 하루전까지만*/
and left(tx_before.chng_dttm,8) between from_timestamp(date_sub(to_timestamp('20240213', 'yyyyMMdd'),1),'yyyyMMdd') and '20240213'
/*24시간 이내*/
and (unix_timestamp(tx.tx_dttm) - unix_timestamp(to_timestamp(rpad(tx_before.chng_dttm,17,'0'), 'yyyyMMddHHmmssSSS')) between 1 and 24*60*60))
group by 1
)
, ac_brf as (
select tx.guid
, nvl(ac_brf_mini.mini_frst_new_dt, ac_brf.ondmnd_frst_new_dt)                        as acco_frst_new_dt           /*계좌최초신규일자*/
, nvl(ac_brf.ondmnd_revi_acco_bal,0)     + nvl(ac_brf_mini.mini_acco_bal,0)           as ondmnd_revi_acco_bal       /*요구불보정계좌잔액  + 미니계좌잔액*/
, nvl(ac_brf.ondmnd_tdd_new_accn,0)      + nvl(ac_brf_mini.mini_tdd_new_accn,0)       as ondmnd_tdd_new_accn        /*요구불당일신규계좌수 + 미니당일신규계좌수*/
, nvl(ac_brf.sfbox_revi_acco_bal,0)                                                   as sfbox_revi_acco_bal        /*세이프박스보정계좌잔액*/
, nvl(ac_brf.sfbox_tdd_new_accn,0)                                                    as sfbox_tdd_new_accn         /*세이프박스당일신규계좌수*/
, nvl(ac_brf.indp_revi_acco_bal,0)       + nvl(ac_brf_mini.dd26_svng_acco_bal,0)      as indp_revi_acco_bal         /*적금보정계좌잔액  + 26일저금계좌잔액*/
, nvl(ac_brf.indp_tdd_new_accn,0)        + nvl(ac_brf_mini.dd26_svng_tdd_new_accn,0)  as indp_tdd_new_accn          /*적금당일신규계좌수 + 26일저금당일신규계좌수*/
, nvl(ac_brf.indp_vld_accn,0)            + nvl(ac_brf_mini.dd26_svng_vld_accn,0)      as indp_vld_accn              /*적금유효계좌수    + 26일저금유효계좌수*/
, nvl(ac_brf.indp_expr_cls_accn,0)       + nvl(ac_brf_mini.dd26_svng_expr_cls_accn,0) as indp_expr_cls_accn         /*적금만기해지계좌수 + 26일저금만기해지계좌수*/
, nvl(ac_brf.indp_mdcl_accn,0)           + nvl(ac_brf_mini.dd26_svng_mdcl_accn,0)     as indp_mdcl_accn             /*적금중도해지계좌수 + 26일저금중도해지계좌수*/
, nvl(ac_brf.wek26_indp_expr_cls_accn,0) + nvl(ac_brf_mini.dd26_svng_cha_scs_accn,0)  as wekdd26_indp_expr_cls_accn /*26주적금만기해지계좌수 + 26일저금도전성공계좌수*/
, nvl(ac_brf.mndp_revi_acco_bal,0)                                                    as mndp_revi_acco_bal         /*예금보정계좌잔액*/
, nvl(ac_brf.mndp_tdd_new_accn,0)                                                     as mndp_tdd_new_accn          /*예금당일신규계좌수*/
, nvl(ac_brf.sharedacc_acco_bal,0)                                                    as sharedacc_acco_bal         /*모임통장계좌잔액*/
, nvl(ac_brf.tdd_new_sharedacc_accn,0)                                                as tdd_new_sharedacc_accn     /*당일신규모임통장계좌수*/
, case when ac_brf.sharedacc_sfbox_open_yn = 'Y' then 1 else 0 end                    as sharedacc_sfbox_open_yn    /*모임통장세이프박스개설여부*/
from anl_workspace.thomas_dataset_c360_tx_infer_target_5min as tx /*C360_수신거래내역 -> inference target 추출*/
left join crm.uccrm_prct_dpcn_brf_s as ac_brf      /*고객별수신계약일요약 --> 2일전 데이터와 조인*/
on (from_timestamp(date_sub(to_timestamp('20240213', 'yyyyMMdd'),2),'yyyyMMdd') = ac_brf.base_dt
and tx.cstno                                                                      = ac_brf.cstno)
left join crm.uccrm_prct_mnac_brf_s as ac_brf_mini /*고객별미니계좌일요약 --> 2일전 데이터와 조인*/
on (from_timestamp(date_sub(to_timestamp('20240213', 'yyyyMMdd'),2),'yyyyMMdd') = ac_brf_mini.base_dt
and tx.cstno                                                                      = ac_brf_mini.cstno)
)
select tx.tx_dttm                                                                                                                                                                            as tx_dttm                      /*거래 일시(timestamp)*/
, tx.tx_lgl_holday_yn                                                                                                                                                                   as tx_lgl_holday_yn             /*법정공휴일 여부*/
, case when tx.tx_bzdd_yn = 'Y' then 1 else 0 end                                                                                                                                       as tx_bzdd_yn                   /*영업일 여부*/
, cast(tx.tx_time_hh as int)                                                                                                                                                            as tx_time_hh                   /*거래 시간대(HH)*/
, case when tx.tx_tmzn = '00_06' then 1 else 0 end                                                                                                                                      as tx_tmzn_0006                 /*거래 시간대-새벽*/
, case when tx.tx_tmzn = '07_11' then 1 else 0 end                                                                                                                                      as tx_tmzn_0711                 /*거래 시간대-오전*/
, case when tx.tx_tmzn = '12_18' then 1 else 0 end                                                                                                                                      as tx_tmzn_1218                 /*거래 시간대-오후*/
, case when tx.tx_tmzn = '19_23' then 1 else 0 end                                                                                                                                      as tx_tmzn_1923                 /*거래 시간대-저녁*/
, tx.cstno                                                                                                                                                                              as cstno                        /*고객번호*/
, tx.guid                                                                                                                                                                               as guid                         /*GUID*/
, nvl(tx.nrom_sta_mncd_ccnt,0)                                                                                                                                                          as nrom_sta_mncd_ccnt           /*정상상태미니카드건수*/
, tx.card_no                                                                                                                                                                            as card_no                      /*카드번호*/
, nvl(tx.card_gds_alnc_cd, 'Unknown')                                                                                                                                                   as card_gds_alnc_cd             /*카드상품제휴코드: 0001 프렌즈 체크카드 / 0002 mini카드 / 0003 개인사업자 체크카드*/
, nvl(datediff(to_timestamp(nvl(tx.frst_card_iss_dt,'20240213'), 'yyyyMMdd'), tx.tx_dttm),-1)                                                                                         as dt_diff_card_new_dt_tx       /*카드신규일자*/
, nvl(datediff(to_timestamp(nvl(tx.card_iss_dt     ,'20240213'), 'yyyyMMdd'), tx.tx_dttm),-1)                                                                                         as dt_diff_card_iss_dt          /*카드발급일자*/
, cast(case when tx.cst_sex_cd = 'F' then 1 when tx.cst_sex_cd = 'M' then 0 else 0.5 end as float)                                                                                      as cst_sex_cd_f                 /*성별*/
, cast(nvl(tx.cst_age,-1) as float)                                                                                                                                                     as cst_age                      /*만나이*/
, nvl(tx.cst_argn,-1)                                                                                                                                                                   as cst_argn                     /*나이대*/
, nvl(tx.cdd_ocup_dvcd,    'Unknown')                                                                                                                                                   as cdd_ocup_dvcd                /*CDD 직업구분코드*/
, cast(nvl(tx.rthn_card_ned_yn, 0.5) as float)                                                                                                                                          as rthn_card_ned_yn             /*실물카드 필요여부*/
, nvl(tx.tx_div_nm, 'Unknown')                                                                                                                                                          as tx_div_nm                    /*거래구분*/
, nvl(tx.tx_mhod_nm, 'Unknown')                                                                                                                                                         as tx_mhod_nm                   /*결제방식*/
, cast(nvl(tx.tx_amt,0) as float)                                                                                                                                                       as tx_amt                       /*거래 총액*/
, tx.uplc_ofln_yn                                                                                                                                                                       as uplc_ofln_yn                 /*사용처-오프라인 여부*/
, case when tx.uplc_div_nm = 'chcap'                               then 1 else 0 end                                                                                                    as uplc_div_chcap_yn            /*사용처구분-신용카드결제 여부*/
, case when left(tx.uplc_div_nm,4) = 'atmc'                        then 1 else 0 end                                                                                                    as uplc_div_atmc_yn             /*사용처구분-ATM거래 여부*/
, case when tx.uplc_div_nm in ('atmc_mnrc','atmc_wdl','atmc_cssr') then 1 else 0 end                                                                                                    as uplc_div_atmc_cashtx_yn      /*사용처구분-ATM현금거래 여부*/
, case when tx.uplc_div_nm = 'atmc_mnrc'                           then 1 else 0 end                                                                                                    as uplc_div_atmc_mnrc_yn        /*사용처구분-ATM입금거래 여부*/
, case when tx.uplc_div_nm = 'atmc_wdl'                            then 1 else 0 end                                                                                                    as uplc_div_atmc_wdl_yn         /*사용처구분-ATM출금거래 여부*/
, case when tx.uplc_div_nm = 'atmc_trsf'                           then 1 else 0 end                                                                                                    as uplc_div_atmc_trsf_yn        /*사용처구분-ATM이체거래 여부*/
, nvl(tx.uplc_nm, 'Unknown')                                                                                                                                                            as uplc_nm                      /*사용처명*/
, nvl(tx.uplc_clss_nm, 'Unknown')                                                                                                                                                       as uplc_clss_nm                 /*사용처_분류명*/
, nvl(cst_tx_24h.cnt,0)                                                                                                                                                                 as tx_cnt_ltly_24h              /*최근24시간 내 거래건수(국내)*/
, cast(nvl(cst_tx_24h.tx_amt,0) as float)                                                                                                                                               as tx_amt_ltly_24h              /*최근24시간 내 거래총액(국내)*/
, cast(nvl(cst_tx_24h.tx_unit_amt,0) as float)                                                                                                                                          as tx_unit_amt_ltly_24h         /*최근24시간 내 평균 거래건단가(국내)*/
, case when cst_tx_24h_abrd.cnt >= 1 then 1 else 0 end                                                                                                                                  as tx_24h_abrd_yn               /*최근24시간 내 해외거래 존재여부*/
, nvl(cst_tx_24h_abrd.cnt,0)                                                                                                                                                            as tx_cnt_ltly_24h_abrd         /*최근24시간 내 거래건수(해외)*/
, cast(nvl(cst_tx_24h_abrd.tx_amt,0) as float)                                                                                                                                          as tx_amt_ltly_24h_abrd         /*최근24시간 내 거래총액(해외)*/
, cast(nvl(cst_tx_24h_abrd.tx_unit_amt,0)             as float)                                                                                                                         as tx_unit_amt_ltly_24h_abrd    /*최근24시간 내 평균 거래건단가(해외)*/
, cast(nvl(cst_tx_90d.tx_time_diff_rbf,365)           as float)                                                                                                                         as tx_time_diff_rbf             /*직전거래로부터 지난 시간*/
, cast(nvl(cst_tx_90d_uplc.tx_time_diff_rbf,365)      as float)                                                                                                                         as tx_time_diff_rbf_uplc        /*같은 사용처의 직전거래로부터 지난 시간*/
, cast(nvl(cst_tx_90d_uplc_clss.tx_time_diff_rbf,365) as float)                                                                                                                         as tx_time_diff_rbf_uplc_clss   /*같은 사용처분류의 직전거래로부터 지난 시간*/
, cast(nvl(tx_3m_cst.tx_cnt_dd_avg,0)                 as float)                                                                                                                         as tx_cnt_dd_avg                /*일평균 거래건수*/
, cast(case when nvl(tx_3m_cst.tx_cnt_dd_avg,0)      != 0 then (nvl(cst_tx_24h.cnt,0)                - tx_3m_cst.tx_cnt_dd_avg)/tx_3m_cst.tx_cnt_dd_avg            else 0 end as float) as tx_cnt_dd_avg_diff_rate      /*일평균 거래건수 대비 최근24시간 거래건수 증감율*/
, cast(nvl(tx_3m_cst.tx_amt_dd_avg,0) as float)                                                                                                                                         as tx_amt_dd_avg                /*일평균 거래총액*/
, cast(case when nvl(tx_3m_cst.tx_amt_dd_avg,0)      != 0 then (nvl(cst_tx_24h.tx_amt,0)             - tx_3m_cst.tx_amt_dd_avg)/tx_3m_cst.tx_amt_dd_avg            else 0 end as float) as tx_amt_dd_avg_diff_rate      /*일평균 거래총액 대비 최근 24시간 거래총액 증감율*/
, cast(nvl(tx_3m_cst.tx_unit_amt_dd_avg,0) as float)                                                                                                                                    as tx_unit_amt_dd_avg           /*일평균 거래건단가*/
, cast(case when nvl(tx_3m_cst.tx_unit_amt_dd_avg,0) != 0 then (nvl(cst_tx_24h.tx_unit_amt,0)        - tx_3m_cst.tx_unit_amt_dd_avg)/tx_3m_cst.tx_unit_amt_dd_avg  else 0 end as float) as tx_unit_amt_dd_avg_diff_rate /*일평균 거래건단가 대비 최근 24시간 거래건단가 증감율*/
, cast(nvl(tx_3m_cst.tx_cycl,365) as float)                                                                                                                                             as tx_cycl                      /*거래주기*/
, cast(case when nvl(tx_3m_cst.tx_cycl,0)            != 0 then (nvl(cst_tx_90d.tx_time_diff_rbf,365) - tx_3m_cst.tx_cycl)/tx_3m_cst.tx_cycl                        else 0 end as float) as tx_cycl_diff_rate            /*거래주기 대비 직전거래로부터 지난 시간 증감율*/
, cast(nvl(case when tx.tx_mhod_nm = 'IC'         then tx_3m_cst.tx_prfc_tx_mhod_ic
when tx.tx_mhod_nm = 'KEYIN'      then tx_3m_cst.tx_prfc_tx_mhod_keyin
when tx.tx_mhod_nm = 'SWIPE'      then tx_3m_cst.tx_prfc_tx_mhod_swipe
when tx.tx_mhod_nm = 'TAG'        then tx_3m_cst.tx_prfc_tx_mhod_tag
when tx.tx_mhod_nm = 'QR'         then tx_3m_cst.tx_prfc_tx_mhod_qr
when tx.tx_mhod_nm = 'SAMSUNGPAY' then tx_3m_cst.tx_prfc_tx_mhod_samsungpay
when tx.tx_mhod_nm = 'LGPAY'      then tx_3m_cst.tx_prfc_tx_mhod_lgpay
end, 0) as float)                                                                                                                                                       as tx_mhod_prfc                 /*해당 거래방식의 선호도*/
, cast(nvl(case when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '00' then tx_3m_cst.tx_prfc_bzdd_y_time_00
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '01' then tx_3m_cst.tx_prfc_bzdd_y_time_01
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '02' then tx_3m_cst.tx_prfc_bzdd_y_time_02
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '03' then tx_3m_cst.tx_prfc_bzdd_y_time_03
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '04' then tx_3m_cst.tx_prfc_bzdd_y_time_04
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '05' then tx_3m_cst.tx_prfc_bzdd_y_time_05
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '06' then tx_3m_cst.tx_prfc_bzdd_y_time_06
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '07' then tx_3m_cst.tx_prfc_bzdd_y_time_07
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '08' then tx_3m_cst.tx_prfc_bzdd_y_time_08
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '09' then tx_3m_cst.tx_prfc_bzdd_y_time_09
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '10' then tx_3m_cst.tx_prfc_bzdd_y_time_10
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '11' then tx_3m_cst.tx_prfc_bzdd_y_time_11
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '12' then tx_3m_cst.tx_prfc_bzdd_y_time_12
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '13' then tx_3m_cst.tx_prfc_bzdd_y_time_13
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '14' then tx_3m_cst.tx_prfc_bzdd_y_time_14
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '15' then tx_3m_cst.tx_prfc_bzdd_y_time_15
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '16' then tx_3m_cst.tx_prfc_bzdd_y_time_16
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '17' then tx_3m_cst.tx_prfc_bzdd_y_time_17
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '18' then tx_3m_cst.tx_prfc_bzdd_y_time_18
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '19' then tx_3m_cst.tx_prfc_bzdd_y_time_19
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '20' then tx_3m_cst.tx_prfc_bzdd_y_time_20
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '21' then tx_3m_cst.tx_prfc_bzdd_y_time_21
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '22' then tx_3m_cst.tx_prfc_bzdd_y_time_22
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '23' then tx_3m_cst.tx_prfc_bzdd_y_time_23
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '00' then tx_3m_cst.tx_prfc_bzdd_n_time_00
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '01' then tx_3m_cst.tx_prfc_bzdd_n_time_01
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '02' then tx_3m_cst.tx_prfc_bzdd_n_time_02
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '03' then tx_3m_cst.tx_prfc_bzdd_n_time_03
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '04' then tx_3m_cst.tx_prfc_bzdd_n_time_04
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '05' then tx_3m_cst.tx_prfc_bzdd_n_time_05
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '06' then tx_3m_cst.tx_prfc_bzdd_n_time_06
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '07' then tx_3m_cst.tx_prfc_bzdd_n_time_07
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '08' then tx_3m_cst.tx_prfc_bzdd_n_time_08
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '09' then tx_3m_cst.tx_prfc_bzdd_n_time_09
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '10' then tx_3m_cst.tx_prfc_bzdd_n_time_10
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '11' then tx_3m_cst.tx_prfc_bzdd_n_time_11
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '12' then tx_3m_cst.tx_prfc_bzdd_n_time_12
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '13' then tx_3m_cst.tx_prfc_bzdd_n_time_13
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '14' then tx_3m_cst.tx_prfc_bzdd_n_time_14
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '15' then tx_3m_cst.tx_prfc_bzdd_n_time_15
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '16' then tx_3m_cst.tx_prfc_bzdd_n_time_16
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '17' then tx_3m_cst.tx_prfc_bzdd_n_time_17
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '18' then tx_3m_cst.tx_prfc_bzdd_n_time_18
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '19' then tx_3m_cst.tx_prfc_bzdd_n_time_19
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '20' then tx_3m_cst.tx_prfc_bzdd_n_time_20
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '21' then tx_3m_cst.tx_prfc_bzdd_n_time_21
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '22' then tx_3m_cst.tx_prfc_bzdd_n_time_22
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '23' then tx_3m_cst.tx_prfc_bzdd_n_time_23
end, 0) as float)                                                                                                                                                       as tx_bzdd_time_hh_prfc /*해당 거래시간대(HH)의 선호도*/
, cast(nvl(case when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) between '00' and '06' then tx_3m_cst.tx_prfc_bzdd_y_tmzn_00_06
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) between '07' and '11' then tx_3m_cst.tx_prfc_bzdd_y_tmzn_07_11
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) between '12' and '18' then tx_3m_cst.tx_prfc_bzdd_y_tmzn_12_18
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) between '19' and '23' then tx_3m_cst.tx_prfc_bzdd_y_tmzn_19_23
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) between '00' and '06' then tx_3m_cst.tx_prfc_bzdd_n_tmzn_00_06
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) between '07' and '11' then tx_3m_cst.tx_prfc_bzdd_n_tmzn_07_11
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) between '12' and '18' then tx_3m_cst.tx_prfc_bzdd_n_tmzn_12_18
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) between '19' and '23' then tx_3m_cst.tx_prfc_bzdd_n_tmzn_19_23
end, 0) as float)                                                                                                                                                        as tx_bzdd_tmzn_prfc    /*해당 거래시간대의 선호도*/
, cast(nvl(tx_3m_cst_uplc_clss.tx_prfc,0)              as float)                                                                                                                                 as uplc_clss_tx_prfc               /*해당 사용처분류의 거래 선호도*/
, cast(nvl(tx_3m_cst_uplc_clss.tx_unit_amt_dd_avg,0)   as float)                                                                                                                                 as uplc_clss_tx_unit_amt_dd_avg    /*해당 사용처분류의 평균 거래건단가*/
, cast(nvl(tx_3m_cst_uplc_clss.tx_unit_amt_dd_stdev,0) as float)                                                                                                                                 as uplc_clss_tx_unit_amt_dd_stdev  /*해당 사용처분류의 거래건단가 표준편차*/
, cast(case when nvl(tx_3m_cst_uplc_clss.tx_unit_amt_dd_avg,0) != 0 then (nvl(tx.tx_amt,0) - tx_3m_cst_uplc_clss.tx_unit_amt_dd_avg)/tx_3m_cst_uplc_clss.tx_unit_amt_dd_avg else 0 end as float) as uplc_clss_tx_unit_amt_diff_rate /*해당 사용처분류의 평균 거래건단가 대비 본건 거래금액 증감율*/
, cast(nvl(tx_3m_cst_uplc_clss.tx_cycl,365)             as float)                                                                                                                                as uplc_clss_tx_cycl               /*해당 사용처분류의 거래주기*/
/*해당 사용처분류의 거래주기 대비 본건 직전거래로부터 지난 시간 증감율*/
, cast(case when nvl(tx_3m_cst_uplc_clss.tx_cycl,0) != 0 then (nvl(cst_tx_90d_uplc_clss.tx_time_diff_rbf,365) - tx_3m_cst_uplc_clss.tx_cycl)/tx_3m_cst_uplc_clss.tx_cycl else 0 end as float)    as uplc_clss_tx_cycl_diff_rate
, cast(nvl(case when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '00' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_00
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '01' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_01
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '02' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_02
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '03' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_03
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '04' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_04
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '05' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_05
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '06' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_06
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '07' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_07
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '08' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_08
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '09' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_09
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '10' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_10
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '11' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_11
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '12' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_12
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '13' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_13
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '14' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_14
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '15' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_15
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '16' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_16
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '17' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_17
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '18' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_18
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '19' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_19
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '20' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_20
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '21' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_21
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '22' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_22
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '23' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_time_23
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '00' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_00
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '01' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_01
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '02' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_02
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '03' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_03
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '04' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_04
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '05' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_05
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '06' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_06
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '07' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_07
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '08' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_08
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '09' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_09
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '10' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_10
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '11' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_11
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '12' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_12
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '13' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_13
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '14' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_14
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '15' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_15
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '16' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_16
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '17' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_17
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '18' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_18
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '19' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_19
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '20' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_20
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '21' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_21
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '22' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_22
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '23' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_time_23
end, 0) as float)                                                                                                                                                        as uplc_clss_tx_bzdd_time_hh_prfc /*해당 사용처분류에 대한 거래 시간대(HH)의 선호도*/
, cast(nvl(case when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) between '00' and '06' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_tmzn_00_06
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) between '07' and '11' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_tmzn_07_11
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) between '12' and '18' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_tmzn_12_18
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) between '19' and '23' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_y_tmzn_19_23
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) between '00' and '06' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_tmzn_00_06
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) between '07' and '11' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_tmzn_07_11
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) between '12' and '18' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_tmzn_12_18
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) between '19' and '23' then tx_3m_cst_uplc_clss.tx_prfc_bzdd_n_tmzn_19_23
end, 0) as float)                                                                                                                                                        as uplc_clss_tx_bzdd_tmzn_prfc    /*해당 사용처분류에 대한 거래 시간대의 선호도*/
, cast(nvl(tx_3m_sex_argn_uplc_clss.tx_prfc,0)              as float)                                                                                                                                        as uplc_clss_tx_prfc_sex_argn               /*해당 사용처분류에 대한 동일 성연령대의 거래 선호도*/
, cast(nvl(tx_3m_sex_argn_uplc_clss.tx_unit_amt_dd_avg,0)   as float)                                                                                                                                        as uplc_clss_tx_unit_amt_dd_avg_sex_argn    /*해당 사용처분류에 대한 동일 성연령대의 평균 거래건단가*/
, cast(nvl(tx_3m_sex_argn_uplc_clss.tx_unit_amt_dd_stdev,0) as float)                                                                                                                                        as uplc_clss_tx_unit_amt_dd_stdev_sex_argn  /*해당 사용처분류에 대한 동일 성연령대의 거래건단가 표준편차*/
, cast(case when nvl(tx_3m_sex_argn_uplc_clss.tx_unit_amt_dd_avg,0) != 0 then (tx.tx_amt - tx_3m_sex_argn_uplc_clss.tx_unit_amt_dd_avg)/tx_3m_sex_argn_uplc_clss.tx_unit_amt_dd_avg else 0 end as float)     as uplc_clss_tx_unit_amt_sex_argn_diff_rate /*해당 사용처분류에 대한 동일 성연령대의 평균 거래건단가 대비 본건 거래금액 증감율*/
, cast(nvl(tx_3m_sex_argn_uplc_clss.tx_cycl,365)             as float)                                                                                                                                       as uplc_clss_tx_cycl_sex_argn               /*해당 사용처분류에 대한 동일 성연령대의 거래주기*/
/*해당 사용처분류에 대한 동일 성연령대의 거래주기 대비 본건 고객의 거래주기 증감율*/
, cast(case when nvl(tx_3m_sex_argn_uplc_clss.tx_cycl,0) != 0 then (nvl(cst_tx_90d_uplc_clss.tx_time_diff_rbf,365) - tx_3m_sex_argn_uplc_clss.tx_cycl)/tx_3m_sex_argn_uplc_clss.tx_cycl else 0 end as float) as uplc_clss_tx_cycl_sex_argn_diff_rate
, cast(nvl(case when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '00' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_00
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '01' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_01
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '02' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_02
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '03' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_03
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '04' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_04
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '05' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_05
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '06' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_06
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '07' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_07
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '08' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_08
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '09' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_09
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '10' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_10
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '11' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_11
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '12' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_12
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '13' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_13
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '14' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_14
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '15' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_15
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '16' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_16
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '17' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_17
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '18' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_18
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '19' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_19
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '20' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_20
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '21' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_21
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '22' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_22
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) = '23' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_time_23
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '00' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_00
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '01' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_01
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '02' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_02
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '03' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_03
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '04' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_04
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '05' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_05
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '06' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_06
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '07' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_07
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '08' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_08
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '09' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_09
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '10' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_10
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '11' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_11
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '12' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_12
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '13' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_13
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '14' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_14
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '15' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_15
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '16' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_16
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '17' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_17
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '18' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_18
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '19' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_19
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '20' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_20
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '21' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_21
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '22' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_22
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) = '23' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_time_23
end, 0) as float)                                                                                                                                                       as uplc_clss_tx_bzdd_time_hh_prfc_sex_argn /*해당 사용처분류에 대한 동일 성연령대의 거래시간대(HH) 선호도*/
, cast(nvl(case when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) between '00' and '06' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_tmzn_00_06
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) between '07' and '11' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_tmzn_07_11
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) between '12' and '18' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_tmzn_12_18
when tx.tx_bzdd_yn = 'Y' and left(tx.tx_time,2) between '19' and '23' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_y_tmzn_19_23
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) between '00' and '06' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_tmzn_00_06
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) between '07' and '11' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_tmzn_07_11
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) between '12' and '18' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_tmzn_12_18
when tx.tx_bzdd_yn = 'N' and left(tx.tx_time,2) between '19' and '23' then tx_3m_sex_argn_uplc_clss.tx_prfc_bzdd_n_tmzn_19_23
end, 0) as float)                                                                                                                                                       as uplc_clss_tx_bzdd_tmzn_prfc_sex_argn /*해당 사용처분류에 대한 동일 성연령대의 거래시간대 선호도*/

, cast(nvl(tx.dstn_bw_onhm_km,-1)                                                                                                                                     as float)         as dstn_bw_onhm_km               /*사용처와 거주지간 거리 (km)*/
, cast(nvl(prep_dstn.dstn_bw_onhm_km_avg,-1)                                                                                                                          as float)         as dstn_bw_onhm_km_avg           /*사용처와 거주지간 평균 거리 (km)*/
, cast(nvl(prep_dstn.dstn_bw_onhm_km_stdev,-1)                                                                                                                        as float)         as dstn_bw_onhm_km_stdev         /*사용처와 거주지간 거리의 표준편차 (km)*/
, cast(nvl(case when nvl(prep_dstn.dstn_bw_onhm_km_avg,0) != 0 then (tx.dstn_bw_onhm_km - prep_dstn.dstn_bw_onhm_km_avg)/prep_dstn.dstn_bw_onhm_km_avg else 0 end,0)  as float)         as dstn_bw_onhm_km_avg_diff_rate /*사용처와 거주지간 평균 거리 대비 본건 사용처와 거주지간 거리 증감율*/
, cast(nvl(tx.dstn_bw_wrst_km,-1)                                                                                                                                     as float)         as dstn_bw_wrst_km               /*사용처와 직장간 거리 (km)*/
, cast(nvl(prep_dstn.dstn_bw_wrst_km_avg,-1)                                                                                                                          as float)         as dstn_bw_wrst_km_avg           /*사용처와 직장간 평균 거리 (km)*/
, cast(nvl(prep_dstn.dstn_bw_wrst_km_stdev,-1)                                                                                                                        as float)         as dstn_bw_wrst_km_stdev         /*사용처와 직장간 거리의 표준편차 (km)*/
, cast(nvl(case when nvl(prep_dstn.dstn_bw_wrst_km_avg,0) != 0 then (tx.dstn_bw_wrst_km - prep_dstn.dstn_bw_wrst_km_avg)/prep_dstn.dstn_bw_wrst_km_avg else 0 end,0)  as float)         as dstn_bw_wrst_km_avg_diff_rate /*사용처와 직장간 평균 거리 대비 본건 사용처와 거주지간 거리 증감율*/
, cast(nvl(sqrt(pow(tx.addr_cdn_uplc_x - prep_dstn.addr_cdn_uplc_x_avg, 2) + pow(tx.addr_cdn_uplc_y - prep_dstn.addr_cdn_uplc_y_avg, 2))/1000,-1)                     as float)         as dstn_bw_uplc_avg_km           /*사용처와 거래중심지간 거리 (km)*/
, cast(nvl(prep_dstn.dstn_bw_uplc_avg_km_avg,-1)                                                                                                                      as float)         as dstn_bw_uplc_avg_km_avg       /*사용처와 거래중심지간 평균 거리 (km)*/
, cast(nvl(prep_dstn.dstn_bw_uplc_avg_km_stdev,-1)                                                                                                                    as float)         as dstn_bw_uplc_avg_km_stdev     /*사용처와 거래중심지간 거리의 표준편차 (km)*/
, cast(nvl(case when nvl(prep_dstn.dstn_bw_uplc_avg_km_avg,0) != 0
then ((sqrt(pow(tx.addr_cdn_uplc_x - prep_dstn.addr_cdn_uplc_x_avg, 2) + pow(tx.addr_cdn_uplc_y - prep_dstn.addr_cdn_uplc_y_avg, 2))/1000) - prep_dstn.dstn_bw_uplc_avg_km_avg)/prep_dstn.dstn_bw_uplc_avg_km_avg else 0 end,0) as float) as dstn_bw_uplc_avg_km_avg_diff_rate /*사용처와 거래중심지간 평균 거리 대비 본건 사용처와 거주지간 거리 증감율*/
, cast(nvl(cst_tx_24h_ofln.dstn_bw_rbf_km,-1)                                                                                                                         as float)         as dstn_bw_rbf_km                /*직전사용처와의 거리 (km)*/
, cast(nvl(prep_dstn.dstn_bw_rbf_km_avg,-1)                                                                                                                           as float)         as dstn_bw_rbf_km_avg            /*사용처간 평균 거리 (km)*/
, cast(nvl(prep_dstn.dstn_bw_rbf_km_stdev,-1)                                                                                                                         as float)         as dstn_bw_rbf_km_stdev          /*사용처간 거리의 표준편차 (km)*/
, cast(nvl(case when nvl(prep_dstn.dstn_bw_rbf_km_avg,0) != 0 then (cst_tx_24h_ofln.dstn_bw_rbf_km - prep_dstn.dstn_bw_rbf_km_avg)/prep_dstn.dstn_bw_rbf_km_avg else 0 end,0) as float) as dstn_bw_rbf_km_avg_diff_rate  /*사용처간 평균 거리 대비 본건의 직전 사용처간 거리 증감율*/
, cast(nvl(case when is_inf(cst_tx_24h_ofln.sped_bw_rbf_kmh) then NULL else cst_tx_24h_ofln.sped_bw_rbf_kmh end,-1)                                                   as float)         as sped_bw_rbf_kmh               /*직전 사용처로부터 이동한 속도 (km/h)*/
, cast(nvl(prep_dstn.sped_bw_rbf_kmh_avg,-1)                                                                                                                          as float)         as sped_bw_rbf_kmh_avg           /*사용처간 평균 이동속도 (km/h)*/
, cast(nvl(prep_dstn.sped_bw_rbf_kmh_stdev,-1)                                                                                                                        as float)         as sped_bw_rbf_kmh_stdev         /*사용처간 이동속도의 표준편차 (km/h)*/
, cast(nvl(case when is_inf(cst_tx_24h_ofln.sped_bw_rbf_kmh) or nvl(prep_dstn.sped_bw_rbf_kmh_avg,0) = 0 then NULL else (cst_tx_24h_ofln.sped_bw_rbf_kmh - prep_dstn.sped_bw_rbf_kmh_avg)/prep_dstn.sped_bw_rbf_kmh_avg end,0) as float) as sped_bw_rbf_kmh_avg_diff_rate /*사용처간 평균 이동속도 대비 본건의 직전 사용처로부터 이동한 속도 증감율*/

, nvl(datediff(to_timestamp(nvl(ac_brf.acco_frst_new_dt,'20240213'), 'yyyyMMdd'), tx.tx_dttm),-1)                                                                      as dt_diff_acco_frst_new_dt          /*계좌최초신규일자*/
, cast(nvl(ac_brf.ondmnd_revi_acco_bal,0) as float)                                                                                                                      as ondmnd_revi_acco_bal              /*요구불보정계좌잔액 + 미니계좌잔액*/
, cast(case when nvl(ac_brf.ondmnd_revi_acco_bal,0) != 0 then (tx.tx_amt/ac_brf.ondmnd_revi_acco_bal) else -1 end as float)                                              as tx_amt_ondmnd_revi_acco_bal_rate  /*요구불보정계좌잔액 + 미니계좌잔액 대비 거래금액*/
, nvl(ac_brf.ondmnd_tdd_new_accn,0)                                                                                                                                      as ondmnd_tdd_new_accn               /*요구불당일신규계좌수 + 미니당일신규계좌수*/
, cast(nvl(ac_brf.sfbox_revi_acco_bal,0) as float)                                                                                                                       as sfbox_revi_acco_bal               /*세이프박스보정계좌잔액*/
, nvl(ac_brf.sfbox_tdd_new_accn,0)                                                                                                                                       as sfbox_tdd_new_accn                /*세이프박스당일신규계좌수*/
, cast(nvl(ac_brf.indp_revi_acco_bal,0) as float)                                                                                                                        as indp_revi_acco_bal                /*적금보정계좌잔액  + 26일저금계좌잔액*/
, nvl(ac_brf.indp_tdd_new_accn,0)                                                                                                                                        as indp_tdd_new_accn                 /*적금당일신규계좌수 + 26일저금당일신규계좌수*/
, nvl(ac_brf.indp_vld_accn,0)                                                                                                                                            as indp_vld_accn                     /*적금유효계좌수    + 26일저금유효계좌수*/
, nvl(ac_brf.indp_expr_cls_accn,0)                                                                                                                                       as indp_expr_cls_accn                /*적금만기해지계좌수 + 26일저금만기해지계좌수*/
, nvl(ac_brf.indp_mdcl_accn,0)                                                                                                                                           as indp_mdcl_accn                    /*적금중도해지계좌수 + 26일저금중도해지계좌수*/
, nvl(ac_brf.wekdd26_indp_expr_cls_accn,0)                                                                                                                               as wekdd26_indp_expr_cls_accn        /*26주적금만기해지계좌수 + 26일저금도전성공계좌수*/
, cast(nvl(ac_brf.mndp_revi_acco_bal,0) as float)                                                                                                                        as mndp_revi_acco_bal                /*예금보정계좌잔액*/
, nvl(ac_brf.mndp_tdd_new_accn,0)                                                                                                                                        as mndp_tdd_new_accn                 /*예금당일신규계좌수*/
, cast(nvl(ac_brf.sharedacc_acco_bal,0) as float)                                                                                                                        as sharedacc_acco_bal                /*모임통장계좌잔액*/
, nvl(ac_brf.tdd_new_sharedacc_accn,0)                                                                                                                                   as tdd_new_sharedacc_accn            /*당일신규모임통장계좌수*/
, nvl(ac_brf.sharedacc_sfbox_open_yn,0)                                                                                                                                  as sharedacc_sfbox_open_yn           /*모임통장세이프박스개설여부*/

, nvl(cst_tx_24h_recv.tx_24h_obnk_otpp_ojbb_acno_mnrc_cnt,0)                       as tx_24h_obnk_otpp_ojbb_acno_mnrc_cnt                 /*최근24시간 타행->당행계좌 입금 건수*/
, cast(nvl(cst_tx_24h_recv.tx_24h_obnk_otpp_ojbb_acno_mnrc_amt,0) as float)        as tx_24h_obnk_otpp_ojbb_acno_mnrc_amt                 /*최근24시간 타행->당행계좌 입금 총액*/
, cast(nvl(cst_tx_24h_recv.tx_24h_obnk_otpp_ojbb_acno_mnrc_time_diff,365)          as float) as tx_24h_obnk_otpp_ojbb_acno_mnrc_time_diff           /*최근24시간 타행->당행계좌 입금으로부터 지난 시간*/
, cast(nvl(cst_tx_24h_recv.tx_24h_obnk_otpp_ojbb_acno_mnrc_dstn_bw_onhm_km,-1)     as float) as tx_24h_obnk_otpp_ojbb_acno_mnrc_dstn_bw_onhm_km     /*최근24시간 타행->당행계좌 입금된 계좌관리점과 거주지간 거리 (km)*/
, cast(nvl(cst_tx_24h_recv.tx_24h_obnk_otpp_ojbb_acno_mnrc_dstn_bw_wrst_km,-1)     as float) as tx_24h_obnk_otpp_ojbb_acno_mnrc_dstn_bw_wrst_km     /*최근24시간 타행->당행계좌 입금된 계좌관리점과 직장간 거리 (km)*/
, cast(nvl(cst_tx_24h_recv.tx_24h_obnk_otpp_ojbb_acno_mnrc_dstn_bw_uplc_avg_km,-1) as float) as tx_24h_obnk_otpp_ojbb_acno_mnrc_dstn_bw_uplc_avg_km /*최근24시간 타행->당행계좌 입금된 계좌관리점과 거래중심지간 거리 (km)*/
, cast(nvl(cst_tx_24h_recv.tx_24h_obnk_otpp_ojbb_acno_mnrc_dstn_bw_uplc_km,-1)     as float) as tx_24h_obnk_otpp_ojbb_acno_mnrc_dstn_bw_uplc_km     /*최근24시간 타행->당행계좌 입금된 계좌관리점과 사용처간 거리 (km)*/
, nvl(cst_tx_24h_recv.tx_24h_atmc_ojbb_acno_mnrc_cnt,0)                                 as tx_24h_atmc_ojbb_acno_mnrc_cnt                 /*최근24시간 자동화기기-> 당행계좌 입금 건수*/
, cast(nvl(cst_tx_24h_recv.tx_24h_atmc_ojbb_acno_mnrc_amt,0) as float)                  as tx_24h_atmc_ojbb_acno_mnrc_amt                 /*최근24시간 자동화기기-> 당행계좌 입금 총액*/
, cast(nvl(cst_tx_24h_recv.tx_24h_atmc_ojbb_acno_mnrc_time_diff,365)          as float) as tx_24h_atmc_ojbb_acno_mnrc_time_diff           /*최근24시간 자동화기기-> 당행계좌 입금으로부터 지난 시간*/
, cast(nvl(cst_tx_24h_recv.tx_24h_atmc_ojbb_acno_mnrc_dstn_bw_onhm_km,-1)     as float) as tx_24h_atmc_ojbb_acno_mnrc_dstn_bw_onhm_km     /*최근24시간 타행->당행계좌 입금된 계좌관리점과 거주지간 거리 (km)*/
, cast(nvl(cst_tx_24h_recv.tx_24h_atmc_ojbb_acno_mnrc_dstn_bw_wrst_km,-1)     as float) as tx_24h_atmc_ojbb_acno_mnrc_dstn_bw_wrst_km     /*최근24시간 타행->당행계좌 입금된 계좌관리점과 직장간 거리 (km)*/
, cast(nvl(cst_tx_24h_recv.tx_24h_atmc_ojbb_acno_mnrc_dstn_bw_uplc_avg_km,-1) as float) as tx_24h_atmc_ojbb_acno_mnrc_dstn_bw_uplc_avg_km /*최근24시간 타행->당행계좌 입금된 계좌관리점과 거래중심지간 거리 (km)*/
, cast(nvl(cst_tx_24h_recv.tx_24h_atmc_ojbb_acno_mnrc_dstn_bw_uplc_km,-1)     as float) as tx_24h_atmc_ojbb_acno_mnrc_dstn_bw_uplc_km     /*최근24시간 타행->당행계좌 입금된 계좌관리점과 사용처간 거리 (km)*/


, nvl(cst_tx_24h_atmc.tx_24h_atmc_wdl_tw10_cnt,0)                                 as tx_24h_atmc_wdl_tw10_cnt                 /*최근24시간 자동화기기 1만원 이하 출금 횟수*/
, cast(nvl(cst_tx_24h_atmc.tx_24h_atmc_wdl_tw10_time_diff,365)          as float) as tx_24h_atmc_wdl_tw10_time_diff           /*최근24시간 자동화기기 1만원 이하 출금으로부터 지난 시간*/
, cast(nvl(cst_tx_24h_atmc.tx_24h_atmc_wdl_tw10_dstn_bw_onhm_km,-1)     as float) as tx_24h_atmc_wdl_tw10_dstn_bw_onhm_km     /*최근24시간 자동화기기 1만원 이하 출금 지점과 거주지간 거리 (km)*/
, cast(nvl(cst_tx_24h_atmc.tx_24h_atmc_wdl_tw10_dstn_bw_wrst_km,-1)     as float) as tx_24h_atmc_wdl_tw10_dstn_bw_wrst_km     /*최근24시간 자동화기기 1만원 이하 출금 지점과 직장간 거리 (km)*/
, cast(nvl(cst_tx_24h_atmc.tx_24h_atmc_wdl_tw10_dstn_bw_uplc_avg_km,-1) as float) as tx_24h_atmc_wdl_tw10_dstn_bw_uplc_avg_km /*최근24시간 자동화기기 1만원 이하 출금 지점과 거래중심지간 거리 (km)*/
, cast(nvl(cst_tx_24h_atmc.tx_24h_atmc_wdl_tw10_dstn_bw_uplc_km,-1)     as float) as tx_24h_atmc_wdl_tw10_dstn_bw_uplc_km     /*최근24시간 자동화기기 1만원 이하 출금 지점과 사용처간 거리 (km)*/


, cast(nvl(cst_tx_24h_lnbz.lnbz_exec_time_diff_nstg,365) as float) as tx_24h_lnbz_exec_time_diff_nstg /*최근24시간 비상금대출 실행일시로부터 지난 시간 (신규/증액)*/
, cast(nvl(cst_tx_24h_lnbz.lnbz_exec_time_diff_cred,365) as float) as tx_24h_lnbz_exec_time_diff_cred /*최근24시간 신용대출 실행일시로부터 지난 시간 (신규/증액)*/
, cast(nvl(cst_tx_24h_lnbz.lnbz_exec_time_diff_grmn,365) as float) as tx_24h_lnbz_exec_time_diff_grmn /*최근24시간 전월세보증금대출 실행일시로부터 지난 시간 (신규/증액)*/
, cast(nvl(cst_tx_24h_lnbz.lnbz_exec_time_diff_rles,365) as float) as tx_24h_lnbz_exec_time_diff_rles /*최근24시간 부동산담보대출 실행일시로부터 지난 시간 (신규/증액)*/
, cast(nvl(cst_tx_24h_lnbz.lnbz_exec_time_diff_busi,365) as float) as tx_24h_lnbz_exec_time_diff_busi /*최근24시간 사업자대출 실행일시로부터 지난 시간 (신규/증액)*/
, cast(nvl(cst_tx_24h_lnbz.lnbz_exec_time_diff_sls ,365) as float) as tx_24h_lnbz_exec_time_diff_sls  /*최근24시간 햇살론대출 실행일시로부터 지난 시간 (신규/증액)*/
, cast(case when cst_tx_24h_lnbz.lnbz_exec_amt_nstg > 0 then tx.tx_amt/cst_tx_24h_lnbz.lnbz_exec_amt_nstg else 0 end as float) as tx_24h_lnbz_exec_amt_nstg /*최근24시간 비상금대출 실행금액 대비 본건 거래금액 (신규/증액)*/
, cast(case when cst_tx_24h_lnbz.lnbz_exec_amt_cred > 0 then tx.tx_amt/cst_tx_24h_lnbz.lnbz_exec_amt_cred else 0 end as float) as tx_24h_lnbz_exec_amt_cred /*최근24시간 신용대출 실행금액 대비 본건 거래금액 (신규/증액)*/
, cast(case when cst_tx_24h_lnbz.lnbz_exec_amt_grmn > 0 then tx.tx_amt/cst_tx_24h_lnbz.lnbz_exec_amt_grmn else 0 end as float) as tx_24h_lnbz_exec_amt_grmn /*최근24시간 전월세보증금대출 실행금액 대비 본건 거래금액 (신규/증액)*/
, cast(case when cst_tx_24h_lnbz.lnbz_exec_amt_rles > 0 then tx.tx_amt/cst_tx_24h_lnbz.lnbz_exec_amt_rles else 0 end as float) as tx_24h_lnbz_exec_amt_rles /*최근24시간 부동산담보대출 실행금액 대비 본건 거래금액 (신규/증액)*/
, cast(case when cst_tx_24h_lnbz.lnbz_exec_amt_busi > 0 then tx.tx_amt/cst_tx_24h_lnbz.lnbz_exec_amt_busi else 0 end as float) as tx_24h_lnbz_exec_amt_busi /*최근24시간 사업자대출 실행금액 대비 본건 거래금액 (신규/증액)*/
, cast(case when cst_tx_24h_lnbz.lnbz_exec_amt_sls  > 0 then tx.tx_amt/cst_tx_24h_lnbz.lnbz_exec_amt_sls  else 0 end as float) as tx_24h_lnbz_exec_amt_sls  /*최근24시간 햇살론대출 실행금액 대비 본건 거래금액 (신규/증액)*/


, cast(case when tx.card_no is null then -1 else nvl(chcr_limt_chng.chcr_limt_chng_time_diff,365) end as float) as tx_24h_chcr_limt_chng_time_diff /*최근24시간 카드한도 변경으로부터 지난 시간*/
,      case when tx.card_no is null then -1 else nvl(chcr_limt_chng.chcr_limt_chng_cnt      ,  0) end           as tx_24h_chcr_limt_chng_cnt       /*최근24시간 카드한도 변경횟수*/


, cast(tx.tx_dt as CHAR(8)) as tx_dt
from anl_workspace.thomas_dataset_c360_tx_infer_target_5min as tx        /*C360_수신거래내역 -> inference target 추출*/

left join cst_tx_90d_uplc_clss
on (tx.guid = cst_tx_90d_uplc_clss.guid)
left join cst_tx_90d_uplc
on (tx.guid = cst_tx_90d_uplc.guid)
left join cst_tx_90d
on (tx.guid = cst_tx_90d.guid)
left join cst_tx_24h
on (tx.guid = cst_tx_24h.guid)
left join cst_tx_24h_abrd
on (tx.guid = cst_tx_24h_abrd.guid)
left join cst_tx_24h_ofln
on (tx.guid = cst_tx_24h_ofln.guid)
left join cst_tx_24h_recv
on (tx.guid = cst_tx_24h_recv.guid)
left join cst_tx_24h_atmc
on (tx.guid = cst_tx_24h_atmc.guid)
left join cst_tx_24h_lnbz
on (tx.guid = cst_tx_24h_lnbz.guid)
left join chcr_limt_chng
on (tx.guid = chcr_limt_chng.guid)
inner join ac_brf
on (tx.guid = ac_brf.guid)
left join anl_workspace.thomas_dataset_tx_dmsc_prep_tx_3m_cst as tx_3m_cst
on (from_timestamp(date_sub(to_timestamp('20240213', 'yyyyMMdd'),2),'yyyyMMdd') = tx_3m_cst.base_dt
and tx.cstno     = tx_3m_cst.cstno)
left join anl_workspace.thomas_dataset_tx_dmsc_prep_tx_3m_cst_uplc_clss as tx_3m_cst_uplc_clss
on (from_timestamp(date_sub(to_timestamp('20240213', 'yyyyMMdd'),2),'yyyyMMdd') = tx_3m_cst_uplc_clss.base_dt
and tx.cstno        = tx_3m_cst_uplc_clss.cstno
and tx.uplc_div_nm  = tx_3m_cst_uplc_clss.uplc_div_nm
and tx.uplc_clss_nm = tx_3m_cst_uplc_clss.uplc_clss_nm)
left join anl_workspace.thomas_dataset_tx_dmsc_prep_tx_3m_sex_argn_uplc_clss as tx_3m_sex_argn_uplc_clss
on (from_timestamp(date_sub(to_timestamp('20240213', 'yyyyMMdd'),2),'yyyyMMdd') = tx_3m_sex_argn_uplc_clss.base_dt
and tx.cst_sex_cd   = tx_3m_sex_argn_uplc_clss.cst_sex_cd
and tx.cst_argn     = tx_3m_sex_argn_uplc_clss.cst_argn
and tx.uplc_div_nm  = tx_3m_sex_argn_uplc_clss.uplc_div_nm
and tx.uplc_clss_nm = tx_3m_sex_argn_uplc_clss.uplc_clss_nm)
left join anl_workspace.thomas_dataset_tx_dmsc_prep_3m_cst_addr_cdn as prep_dstn
on (from_timestamp(date_sub(to_timestamp('20240213', 'yyyyMMdd'),2),'yyyyMMdd') = prep_dstn.base_dt
and tx.cstno = prep_dstn.cstno)
;