

with tagedtable as (
    select
        t1.policy_no
        ,t1.cvrg_cd
        ,t1.cvrg_unique_cd
        ,t1.valid_policy_ind
        ,t1.policy_status
        ,t1.policy_status_reason
        ,t1.main_rider_ind
        ,t1.app_dt
        ,t1.policy_eff_tm
        ,t1.insurance_period_beg_dt
        ,t1.insurance_period_end_dt
        ,t1.insurance_yearnum
        ,t1.policy_end_dt
        ,t1.surrender_tm
        ,t1.apdprem_cur
        ,t1.app_no
        ,t1.app_age
        ,t1.appnt_src_cust_no
        ,t1.appnt_id_type
        ,t1.appnt_id_no
        ,t1.appnt_insured_rela
        ,t1.insured_src_cust_no
        ,t1.insured_id_type
        ,t1.insured_id_no
        ,t1.policy_insured_occup_cd
        ,t1.prem_rate_level
        ,t1.co_insured_num
        ,t1.bnf_desc
        ,t1.bnf_desc_mode
        ,t1.pieces
        ,t1.policy_level
        ,t1.currency
        ,t1.pstdp_cur
        ,t1.pstdep_cur
        ,t1.punstdep_cur
        ,t1.fst_invoice_status_ind
        ,t1.divdnd_distr_cd
        ,t1.universal_prem_ind
        ,t1.sales_no
        ,t1.csr_no
        ,t1.operator_no
        ,t1.digital_sign_ind
        ,t1.special_agreement
        ,t1.sub_agt_no
        ,t1.sale_type
        ,t1.sale_prod_cd
        ,t1.policy_saleattr_cd
        ,t1.saleattr_indvgrp_ind
        ,t1.policy_indvgrp_ind
        ,t1.renewal_ind
        ,t1.renewal_dt
        ,t1.waiver_prem_rate
        ,t1.supply_input_ind
        ,t1.card_ind
        ,t1.hesi_surr_ind
        ,t1.spec_effect_dt_ind
        ,t1.pro_lf_cross_cd
        ,t1.comb_policy_no
        ,t1.comb_policy_ind
        ,t1.group_no
        ,t1.prepay_clause_choice
        ,t1.policy_branch
        ,t1.gte26d_eff_policy_ind
        ,t1.jinfu_policy_ind
        ,t1.split_bill_ind
        ,t1.split_policy_no
        ,t1.hang_bill_ind
        ,t1.hang_policy_no
        ,t1.etl_tm
        ,t1.etl_src_sys
        ,t1.etl_src_tbl
        ,t1.near_source_ind
        ,t1.branch
        ,case
            when t2.policyno is null and t2.classcode is null then '0'  --???????????????
            when substr(t2.endtime,0,8) !='99991231' then '1'           --?????????????????????
        end
    from dwd_ag_oper_policy_bacic_info_his_di t1
    left join dmgr_prod.v_ex_p10ids_riskcon t2
    on      t1.policy_no =t2.policyno
    and     t1.cvrg_cd = t2.classcode
    and     t2.pt='${v_dt}'
    where   t1.dt='99991231' --?????????????????????

    union all

    --???????????????????????????
    select *
    from ${ods}.v_ex_p10ids_riskcon t1
    left join ${cdm}.dim_pub_metacode_mapping_dd t2
        on t1.aidtype =  t2.src_vale
        and t2.dt = max_pt('${cdm}.dim_pub_metacode_mapping_dd')
        and t2.valid_ind = '1'
        and t2.src_project_name ='dmgr_prod'
        and t2.src_talbe_name='v_ex_p10ids_riskcon'
        and t2.src_column='aitype'
        and t2.targ_table_name = 'dwd_ag_oper_policy_bacic_info_his_di'
        and t2.targ_colum_name = 'appnt_id_type'
    left join ${cdm}.dim_pub_metacode_mapping_dd t3
              on t1.aidtype =  t2.src_vale
                  and t2.dt = max_pt('${cdm}.dim_pub_metacode_mapping_dd')
                  and t2.valid_ind = '1'
                  and t2.src_project_name ='dmgr_prod'
                  and t2.src_talbe_name='v_ex_p10ids_riskcon'
                  and t2.src_column='itype'
                  and t2.targ_table_name = 'dwd_ag_oper_policy_bacic_info_his_di'
                  and t2.targ_colum_name = 'insured_id_type'
    left join ${cdm}.tmp_split_policy3 t4
        on t1.policy

)