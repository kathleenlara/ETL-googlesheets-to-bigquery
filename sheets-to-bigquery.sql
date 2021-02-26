/*

Benchmark from Google Sheets to Google Big Query
Schedule: Done after the data refresh - every hour

*/
  ------------------------------------------------------------------------------------
  -- Campaign Goals Sheet - This is where we collect the inputs from Google Sheets to BQ to be compared against data in Big Query
  ------------------------------------------------------------------------------------
UPDATE
  `project-name.dataset-name.prod_event_trigger_log` u
SET
  campaign = TRIM(c.campaign)
FROM
  `project-name.dataset-name.dailybreak_staging.campaign_goals`c
WHERE
  LOWER(TRIM(u.permalink)) = LOWER(TRIM(c.permalink));
 
UPDATE `project-name.dataset-name.prod_event_trigger_log` u
SET engagement_goal = cast(be.egoal as numeric)
, avg_time_in_secs = be.avg_time
FROM  (
  SELECT e.datestampidx, e.event_name, e.rundateid, e.rundate, e.permalink, eng.campaign, eng.egoal , eng.avg_time
  FROM `project-name.dataset-name.prod_event_trigger_log` e
  JOIN (  SELECT r.permalink , r.event_name, c.campaign, min(r.rundate) as start, max(rundate) as enddate
          , sum(r.break_engagements) as total_engagements, c.engagement_goal
          , round(c.engagement_goal / sum(r.break_engagements),4) as egoal
          , c.Average_Time_Spent_in_seconds as avg_time 
          FROM `project-name.dataset-name.prod_event_trigger_log` r
          JOIN `project-name.dataset-name.campaign_goals`c
          ON lower(trim(r.permalink)) = lower(trim(c.permalink))
          WHERE c.engagement_goal > 0 and r.break_engagements > 0
          and r.rundate BETWEEN c.start_date and c.end_date 
          GROUP BY r.permalink, c.campaign, r.event_name, c.engagement_goal,  c.Total_Engagements , c.Average_Time_Spent_in_seconds
  ) eng
  ON lower(trim(eng.permalink)) = lower(trim(e.permalink))
  and e.rundate BETWEEN eng.start and eng.enddate
  WHERE e.break_engagements > 0
  and e.datestampidx not in 
  (Select e.datestampidx --, count(e.datestampidx) as recCnt
  FROM `project-name.dataset-name.prod_event_trigger_log` e
    JOIN `project-name.dataset-name.campaign_goals`c
          ON lower(trim(e.permalink)) = lower(trim(c.permalink))
      WHERE e.break_engagements > 0
      GROUP BY e.datestampidx
      HAVING count(e.datestampidx) > 1)
) be
WHERE u.datestampidx = be. datestampidx
and lower(trim(u.permalink)) = lower(trim(be.permalink))
and lower(trim(u.event_name)) = lower(trim(be.event_name))
;

------------------------------------------------------------------------------------
-- Campaign Values Sheet - This is where we collect values from Campaigns Sheet to BQ
------------------------------------------------------------------------------------
   
UPDATE `project-name.dataset-name.prod_event_trigger_log` u
SET total_transfers = cast(be.etotal_transfers as numeric)
, product_transfers = cast(be.eproduct_transfers as numeric)
, widgets_sold = cast(be.ewidgets_sold as numeric)
, status = be.status
FROM  ( 
  SELECT e.datestampidx, e.event_name, e.permalink -- , eng.campaign
  , eng.etotal_transfers, eng.eproduct_transfers, eng.ewidgets_sold, eng.status 
  FROM `project-name.dataset-name.prod_event_trigger_log` e
  JOIN ( 
       SELECT r.permalink, r.event_name, min(r.rundate) as start, max(rundate) as enddate
       , sum(r.break_engagements) as total_engagements
       , v.total_transfers, v.product_transfers, v.widgets_sold
      , round(v.total_transfers/ sum(r.break_engagements),4) as etotal_transfers
      , round(v.product_transfers/ sum(r.break_engagements),4) as eproduct_transfers 
      , round(v.widgets_sold/ sum(r.break_engagements),4) as ewidgets_sold
      , v.status 
        FROM `project-name.dataset-name.prod_event_trigger_log` r
        JOIN 
       (SELECT c.permalink --, c.campaign
       , sum(IF(lower(c.label) = 'total transfers', cast(c.value as float64), 0)) as total_transfers  
       , sum(IF(lower(c.label) = 'product transfer', cast(c.value as int64), 0)) as product_transfers
      , sum(IF(lower(c.label) = 'widgets sold', cast(c.value as int64), 0)) as widgets_sold
      , IF(lower(c.label) = 'status', c.value, "na") as status 
        FROM  `project-name.dataset-name.campaign_values`c
        WHERE c.value is not null  -- GROUP BY c.permalink, c.campaign, IF(lower(c.label) = 'status', c.value, "na")) v
      GROUP BY c.permalink, IF(lower(c.label) = 'status', c.value, "na")) v
        ON lower(trim(r.permalink)) = lower(trim(v.permalink))
        WHERE r.break_engagements > 0
        GROUP BY  r.permalink, r.event_name, v.total_transfers, v.product_transfers, v.widgets_sold, v.status 
  	) eng
  	ON lower(trim(eng.permalink)) = lower(trim(e.permalink))
  	and e.rundate BETWEEN eng.start and eng.enddate
  	WHERE e.break_engagements > 0
    and e.datestampidx not in -- ('20201214080434.117090', '20210118081516.067719', '20201205060926.989054')
  (Select e.datestampidx --, count(e.datestampidx) as recCnt
  FROM `project-name.dataset-name.prod_event_trigger_log` e
    JOIN `project-name.dataset-name.campaign_goals`c
          ON lower(trim(e.permalink)) = lower(trim(c.permalink))
      WHERE e.break_engagements > 0
      GROUP BY e.datestampidx
      HAVING count(e.datestampidx) > 1)
) be
WHERE u.datestampidx = be. datestampidx
and lower(trim(u.permalink)) = lower(trim(be.permalink))
and lower(trim(u.event_name)) = lower(trim(be.event_name))
;

      ------------------------------------------------------------------------------------
      -- Benchmarks - This is where we compare collect benchmark values from Sheets to BQ to compare with performance
      ------------------------------------------------------------------------------------
    UPDATE
      `project-name.dataset-name.prod_event_trigger_log` u
    SET
      step_type_definition = LOWER(TRIM(b.step_label))
    FROM
      `project-name.dataset-name.benchmark` b
    WHERE
      LOWER(TRIM(u.step_id)) = LOWER(TRIM(b.step_id))
      AND (u.event_name IN ('step_start',
          'step_interaction')) ;
    UPDATE
      `project-name.dataset-name.prod_event_trigger_log` u
    SET
      step_interaction_benchmark = be.benchmark,
      step_interaction_pct = CAST(be.einteraction_pct AS numeric),
      step_interaction_vs_benchmark = CAST(be.perf_v_benchmark AS numeric)
    FROM (
        -- -------------------------------------------------------------------
      SELECT
        DISTINCT e.datestampidx,
        e.event_name,
        e.uid,
        e.permalink,
        e.step_id,
        ev.benchmark,
        CASE ev.step_interactions
          WHEN 0 THEN 0
        ELSE
        CAST(((e.step_interactions / ev.step_interactions) * ev.interaction_pct) AS numeric)
      END
        AS einteraction_pct,
        CASE e.step_interactions
          WHEN 0 THEN 0
        ELSE
        CASE ev.total_step_starts
          WHEN 0 THEN 0
        ELSE
        CAST ( ((e.step_interactions / ev.total_step_starts) - ev.benchmark) / ev.benchmark AS numeric)
      END
      END
        AS perf_v_benchmark
      FROM
        `project-name.dataset-name.prod_event_trigger_log` e
      JOIN (
        SELECT
          DISTINCT r.permalink,
          r.step_id,
          MIN(r.rundate) AS start,
          MAX(rundate) AS enddate,
          b.benchmark,
          SUM(r.step_starts) AS total_step_starts,
          SUM(r.step_interactions) AS step_interactions,
          CASE SUM(r.step_starts)
            WHEN 0 THEN 0
          ELSE
          (SUM(r.step_interactions) / SUM(r.step_starts))
        END
          AS interaction_pct
        FROM
          `project-name.dataset-name.prod_event_trigger_log` r
        JOIN
          `project-name.dataset-name.benchmark` b
        ON
          LOWER(TRIM(r.step_id)) = LOWER(TRIM(b.step_id))
        WHERE
          b.benchmark IS NOT NULL
          AND (r.event_name IN ('step_start',
              'step_interaction'))
        GROUP BY
          r.permalink,
          r.step_id,
          b.benchmark ) ev
      ON
        LOWER(TRIM(ev.permalink)) = LOWER(TRIM(e.permalink))
        AND LOWER(TRIM(ev.step_id)) = LOWER(TRIM(e.step_id))
        AND e.rundate BETWEEN ev.start
        AND ev.enddate
      WHERE
        ( e.event_name IN ('step_start',
            'step_interaction'))
        -- -----------------------------------------------------------------------
        ) be
    WHERE
      u.datestampidx = be. datestampidx
      AND u.uid = be.uid
      AND LOWER(TRIM(u.permalink)) = LOWER(TRIM(be.permalink))
      AND LOWER(TRIM(u.event_name)) = LOWER(TRIM(be.event_name))
      AND LOWER(TRIM(u.step_id)) = LOWER(TRIM(be.step_id)) ;



