    DROP TABLE IF EXISTS temp_agg_child_health_rhit;
    CREATE UNLOGGED TABLE temp_agg_child_health_rhit AS (select state_id,supervisor_id,awc_id,month,gender,age_tranche,caste,disabled,minority,resident, rations_21_plus_distributed, days_ration_given_child from agg_child_health where 1=0);
    SELECT create_distributed_table('temp_agg_child_health_rhit', 'supervisor_id');
    INSERT INTO temp_agg_child_health_rhit(
            state_id,
            supervisor_id,
            awc_id,
            month,
            gender,
            age_tranche,
            caste,
            disabled,
            minority,
            resident,
            rations_21_plus_distributed,
            days_ration_given_child
    )
    (
        select
            state_id,
            supervisor_id,
            awc_id,
            month,
            sex,
            age_tranche,
            caste,
            COALESCE(chm.disabled, 'no') as coalesce_disabled,
            COALESCE(chm.minority, 'no') as coalesce_minority,
            COALESCE(chm.resident, 'no') as coalesce_resident,
            SUM(CASE WHEN chm.num_rations_distributed >= 21 THEN 1 ELSE 0 END) as rations_21_plus_distributed,
            SUM(chm.days_ration_given_child) as days_ration_given_child
        from child_health_monthly chm
            WHERE month = '{month}' AND state_id='{state_id}'
        group by  state_id,
            supervisor_id,
            awc_id,
            month,
            sex,
            age_tranche,
            caste,
            coalesce_disabled,
            coalesce_minority,
            coalesce_resident
    );
    
    
    DROP TABLE IF EXISTS temp_agg_child;
    CREATE UNLOGGED TABLE temp_agg_child AS (select * from temp_agg_child_health_rhit);
    UPDATE agg_child_health
        SET
            rations_21_plus_distributed = thr_temp.rations_21_plus_distributed,
            days_ration_given_child = thr_temp.days_ration_given_child
    
    from temp_agg_child thr_temp
    where
        agg_child_health.state_id = thr_temp.state_id AND
        agg_child_health.supervisor_id = thr_temp.supervisor_id AND
        agg_child_health.awc_id = thr_temp.awc_id AND
        agg_child_health.month = thr_temp.month AND
        agg_child_health.gender = thr_temp.gender AND
        agg_child_health.age_tranche = thr_temp.age_tranche AND
        agg_child_health.caste = thr_temp.caste AND
        agg_child_health.disabled = thr_temp.disabled AND
        agg_child_health.minority = thr_temp.minority AND
        agg_child_health.resident = thr_temp.resident AND
        agg_child_health.aggregation_level = 5 AND
        agg_child_health.month='{month}' AND
        agg_child_health.state_id='{state_id}'
