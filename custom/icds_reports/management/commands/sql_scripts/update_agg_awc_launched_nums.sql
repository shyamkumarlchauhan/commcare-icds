DROP TABLE IF EXISTS temp_agg_awc_challa;
CREATE UNLOGGED TABLE temp_agg_awc_challa AS (select awc_id, supervisor_id, open_count as cases_household, count(*) as all_cases_household
from "ucr_icds-cas_static-household_cases_eadc276d" where 1=0 group by awc_id, supervisor_id, open_count);;

SELECT create_distributed_table('temp_agg_awc_challa', 'supervisor_id');



INSERT INTO temp_agg_awc_challa (
    awc_id,
    supervisor_id,
    cases_household,
    all_cases_household
)
(
SELECT
            awc_id,
            supervisor_id,
            sum(open_count) AS cases_household,
            count(*) AS all_cases_household
            FROM "ucr_icds-cas_static-household_cases_eadc276d"
            WHERE opened_on<= '{month_end}'
            GROUP BY awc_id, supervisor_id
);
DROP TABLE IF EXISTS temp_agg_awc;
CREATE UNLOGGED TABLE temp_agg_awc AS (select * from temp_agg_awc_challa);

UPDATE agg_awc SET
           cases_household = ut.cases_household,
           is_launched = CASE WHEN ut.all_cases_household>0 THEN 'yes' ELSE 'no' END,
           num_launched_states = CASE WHEN ut.all_cases_household>0 THEN 1 ELSE 0 END,
           num_launched_districts = CASE WHEN ut.all_cases_household>0 THEN 1 ELSE 0 END,
           num_launched_blocks = CASE WHEN ut.all_cases_household>0 THEN 1 ELSE 0 END,
           num_launched_supervisors = CASE WHEN ut.all_cases_household>0 THEN 1 ELSE 0 END,
           num_launched_awcs = CASE WHEN ut.all_cases_household>0 THEN 1 ELSE 0 END
        FROM  temp_agg_awc  ut
        WHERE ut.awc_id = agg_awc.awc_id and ut.supervisor_id=agg_awc.supervisor_id and agg_awc.aggregation_level=5;;


-- updating aggregation level 4 (supervisor)
UPDATE agg_awc SET
           cases_household = ut.cases_household,
           is_launched = CASE WHEN ut.num_launched_supervisors>0 THEN 'yes' ELSE 'no' END,
           num_launched_states = ut.num_launched_states,
           num_launched_districts = ut.num_launched_districts,
           num_launched_blocks = ut.num_launched_blocks,
           num_launched_supervisors = ut.num_launched_supervisors,
           num_launched_awcs = ut.num_launched_awcs
        FROM ( SELECT
           month,
           supervisor_id,
           sum(cases_household) as cases_household,
           sum(num_launched_states) as num_launched_states,
           sum(num_launched_districts) as num_launched_districts,
           sum(num_launched_blocks) as num_launched_blocks,
           sum(num_launched_supervisors) as num_launched_supervisors,
           sum(num_launched_awcs) as num_launched_awcs
            FROM agg_awc
            WHERE month = '{month}' and aggregation_level = 5
            GROUP BY month, supervisor_id) ut
        WHERE ut.supervisor_id=agg_awc.supervisor_id and ut.month=agg_awc.month and agg_awc.month='{month}' and agg_awc.aggregation_level=4;;

-- updating aggregation level 3 (block)
UPDATE agg_awc SET
           cases_household = ut.cases_household,
           is_launched = CASE WHEN ut.num_launched_blocks>0 THEN 'yes' ELSE 'no' END,
           num_launched_states = ut.num_launched_states,
           num_launched_districts = ut.num_launched_districts,
           num_launched_blocks = ut.num_launched_blocks,
           num_launched_supervisors = ut.num_launched_supervisors,
           num_launched_awcs = ut.num_launched_awcs
        FROM ( SELECT
           month,
           block_id,
           sum(cases_household) as cases_household,
           sum(num_launched_states) as num_launched_states,
           sum(num_launched_districts) as num_launched_districts,
           sum(num_launched_blocks) as num_launched_blocks,
           sum(num_launched_supervisors) as num_launched_supervisors,
           sum(num_launched_awcs) as num_launched_awcs
            FROM agg_awc
            WHERE month = '{month}' and aggregation_level = 4
            GROUP BY month, block_id) ut
        WHERE ut.block_id=agg_awc.block_id and ut.month=agg_awc.month and agg_awc.month='{month}' and agg_awc.aggregation_level=3;;

-- updating aggregation level 2 (district)
UPDATE agg_awc SET
           cases_household = ut.cases_household,
           is_launched = CASE WHEN ut.num_launched_districts>0 THEN 'yes' ELSE 'no' END,
           num_launched_states = ut.num_launched_states,
           num_launched_districts = ut.num_launched_districts,
           num_launched_blocks = ut.num_launched_blocks,
           num_launched_supervisors = ut.num_launched_supervisors,
           num_launched_awcs = ut.num_launched_awcs
        FROM ( SELECT
           month,
           district_id,
           sum(cases_household) as cases_household,
           sum(num_launched_states) as num_launched_states,
           sum(num_launched_districts) as num_launched_districts,
           sum(num_launched_blocks) as num_launched_blocks,
           sum(num_launched_supervisors) as num_launched_supervisors,
           sum(num_launched_awcs) as num_launched_awcs
            FROM agg_awc
            WHERE month = '{month}' and aggregation_level = 3
            GROUP BY month, district_id) ut
        WHERE ut.district_id=agg_awc.district_id and ut.month=agg_awc.month and agg_awc.month='{month}' and agg_awc.aggregation_level=2;;


-- updating aggregation level 1 (state)
UPDATE agg_awc SET
           cases_household = ut.cases_household,
           is_launched = CASE WHEN ut.num_launched_districts>0 THEN 'yes' ELSE 'no' END,
           num_launched_states = ut.num_launched_states,
           num_launched_districts = ut.num_launched_districts,
           num_launched_blocks = ut.num_launched_blocks,
           num_launched_supervisors = ut.num_launched_supervisors,
           num_launched_awcs = ut.num_launched_awcs
        FROM ( SELECT
           state_id,
           month,
           sum(cases_household) as cases_household,
           sum(num_launched_states) as num_launched_states,
           sum(num_launched_districts) as num_launched_districts,
           sum(num_launched_blocks) as num_launched_blocks,
           sum(num_launched_supervisors) as num_launched_supervisors,
           sum(num_launched_awcs) as num_launched_awcs
            FROM agg_awc
            WHERE month = '{month}' and aggregation_level = 2
            GROUP BY month, state_id) ut
        WHERE ut.state_id=agg_awc.state_id and ut.month=agg_awc.month and agg_awc.month='{month}' and agg_awc.aggregation_level=1;;

-- Deleting temo tables
DROP TABLE IF EXISTS temp_agg_awc_challa;;

DROP TABLE IF EXISTS temp_agg_awc;
