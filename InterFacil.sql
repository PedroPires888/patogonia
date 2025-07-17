CREATE OR REPLACE VIEW DM_DECISION_ANALYTICS.BENEFIT_TRACKING.INTRANETWORK_LANE_DATA_BY_PERIOD_RESULTS AS 
WITH shipments AS (
    SELECT 
        ship_lane,
        fiscal_period,
        fiscal_yr_id AS fiscal_year,
        fiscal_week_id,
        go_live_flag,
        SUM(lh_trailers+sto_trailers) AS trailers,
        SUM(lh_trailers) AS lh_trailers,
        SUM(sto_trailers) AS sto_trailers,
        SUM(IFNULL(lh_spend,0)+IFNULL(sto_spend,0)) AS cost,
        SUM(IFNULL(lh_spend,0)) AS lh_spend,
        SUM(IFNULL(sto_spend,0)) AS sto_spend,
        SUM(IFNULL(lh_weight,0)+IFNULL(sto_weight,0)) AS weight,
        SUM(IFNULL(lh_cube,0)+IFNULL(sto_cube,0)) AS ttl_cube,
        SUM(IFNULL(lh_cogs,0)+IFNULL(sto_cogs,0)) AS ttl_cogs
    FROM DM_DECISION_ANALYTICS.BENEFIT_TRACKING.INTERFACILITY_DATA
    WHERE fiscal_yr_id IN ('2024','2025')
    AND RIGHT(fiscal_week_id,2) BETWEEN 1 AND 13
    GROUP BY ALL
), 

year_2024 AS (
    SELECT
        ship_lane AS lane,
        fiscal_period,
        MAX(go_live_flag) AS go_live_flag,
        SUM(ttl_cogs) AS cogs_2024,
        SUM(cost) AS cost_2024,
        SUM(trailers) AS trailers_2024,
        SUM(ttl_cube) AS cube_2024,
        
        /* Calculate freight as % of COGS */
        CASE WHEN SUM(ttl_cogs) = 0 THEN 0
             ELSE SUM(cost) / SUM(ttl_cogs) END AS freight_pct_cogs_2024,
        
        /* Calculate other useful metrics */
        CASE WHEN SUM(trailers) = 0 THEN 0
             ELSE SUM(cost) / SUM(trailers) END AS cost_per_trailer_2024,
             
        CASE WHEN SUM(trailers) = 0 THEN 0
             ELSE SUM(ttl_cube) / SUM(trailers) END AS cube_per_trailer_2024
    FROM shipments
    WHERE fiscal_year = '2024'
    GROUP BY ship_lane, fiscal_period
),

year_2025 AS (
    SELECT
        ship_lane AS lane,
        fiscal_period,
        MAX(go_live_flag) AS go_live_flag,
        SUM(ttl_cogs) AS cogs_2025,
        SUM(cost) AS cost_2025,
        SUM(trailers) AS trailers_2025,
        SUM(ttl_cube) AS cube_2025,
        
        /* Calculate freight as % of COGS */
        CASE WHEN SUM(ttl_cogs) = 0 THEN 0
             ELSE SUM(cost) / SUM(ttl_cogs) END AS freight_pct_cogs_2025,
             
        /* Calculate other useful metrics */
        CASE WHEN SUM(trailers) = 0 THEN 0
             ELSE SUM(cost) / SUM(trailers) END AS cost_per_trailer_2025,
             
        CASE WHEN SUM(trailers) = 0 THEN 0
             ELSE SUM(ttl_cube) / SUM(trailers) END AS cube_per_trailer_2025
    FROM shipments
    WHERE fiscal_year = '2025'
    GROUP BY ship_lane, fiscal_period
),

joined AS (
    SELECT
        y0.lane,
        y0.fiscal_period,
        COALESCE(y1.go_live_flag, y0.go_live_flag) AS go_live_flag,
        y0.cogs_2024,
        y0.cost_2024,
        y0.trailers_2024,
        y0.cube_2024,
        y0.freight_pct_cogs_2024,
        y0.cost_per_trailer_2024,
        y0.cube_per_trailer_2024,
        
        y1.cogs_2025,
        y1.cost_2025,
        y1.trailers_2025,
        y1.cube_2025,
        y1.freight_pct_cogs_2025,
        y1.cost_per_trailer_2025,
        y1.cube_per_trailer_2025
    FROM year_2024 y0
    JOIN year_2025 y1 
        ON y0.lane = y1.lane 
        AND y0.fiscal_period = y1.fiscal_period
),

/* Calculate lane-mix effect */
lane_mix_base AS (
    SELECT 
        fiscal_period,
        SUM(cogs_2024) AS total_cogs_2024,
        SUM(cogs_2025) AS total_cogs_2025
    FROM joined
    GROUP BY fiscal_period
),

lane_mix_calc AS (
    SELECT
        j.lane,
        j.fiscal_period,
        j.cogs_2024,
        j.cogs_2025,
        lmb.total_cogs_2024,
        lmb.total_cogs_2025,
        
        /* Lane mix percentages */
        CASE WHEN lmb.total_cogs_2024 = 0 THEN 0
             ELSE j.cogs_2024 / lmb.total_cogs_2024 END AS lane_mix_pct_2024,
        
        CASE WHEN lmb.total_cogs_2025 = 0 THEN 0
             ELSE j.cogs_2025 / lmb.total_cogs_2025 END AS lane_mix_pct_2025,
             
        /* Hypothetical cost if 2024 lane mix was used with 2025 rates */
        CASE WHEN lmb.total_cogs_2024 = 0 THEN 0
             ELSE (j.cogs_2024 / lmb.total_cogs_2024) * lmb.total_cogs_2025 * j.freight_pct_cogs_2025 END 
             AS hypothetical_cost_2024_mix_2025_rate,
             
        /* Hypothetical cost if 2025 lane mix was used with 2024 rates */
        CASE WHEN lmb.total_cogs_2025 = 0 THEN 0 
             ELSE (j.cogs_2025 / lmb.total_cogs_2025) * lmb.total_cogs_2025 * j.freight_pct_cogs_2024 END
             AS hypothetical_cost_2025_mix_2024_rate
    FROM joined j
    JOIN lane_mix_base lmb ON j.fiscal_period = lmb.fiscal_period
),

waterfall AS (
    SELECT
        j.lane,
        j.fiscal_period,
        j.go_live_flag,
        j.cogs_2024,
        j.cost_2024,
        j.freight_pct_cogs_2024,
        j.cogs_2025,
        j.cost_2025,
        j.freight_pct_cogs_2025,
        j.trailers_2024,
        j.trailers_2025,
        j.cube_per_trailer_2024,
        j.cube_per_trailer_2025,
        j.cost_per_trailer_2024,
        j.cost_per_trailer_2025,
        
        /* Lane mix metrics */
        lmc.lane_mix_pct_2024,
        lmc.lane_mix_pct_2025,
        lmc.hypothetical_cost_2024_mix_2025_rate,
        lmc.hypothetical_cost_2025_mix_2024_rate,
        
        /* Lane mix effect: What if we kept 2024 lane mix but used 2025 rates? */
        (lmc.hypothetical_cost_2024_mix_2025_rate - j.cost_2024) AS lane_mix_effect,
        
        /* Actual difference */
        (j.cost_2025 - j.cost_2024) AS total_cost_diff,
        
        /* Define minimum thresholds for reliable analysis */
        CASE 
            WHEN j.cogs_2024 < 10000 OR j.cogs_2025 < 10000 THEN 'Low'
            WHEN j.cogs_2024 < 50000 OR j.cogs_2025 < 50000 THEN 'Medium'
            ELSE 'High'
        END AS volume_confidence,
        
        /* Cap freight percentage at reasonable levels */
        LEAST(j.freight_pct_cogs_2024, 0.50) AS capped_freight_pct_2024,
        LEAST(j.freight_pct_cogs_2025, 0.50) AS capped_freight_pct_2025,
        
        /* Effect 1: COGS Volume Effect - if COGS changed but rate stayed the same */
        CASE
            WHEN j.cogs_2024 < 10000 OR j.cogs_2025 < 10000 THEN 
                (j.cogs_2025 * LEAST(j.freight_pct_cogs_2024, 0.50) - j.cost_2024)
            ELSE 
                (j.cogs_2025 * j.freight_pct_cogs_2024 - j.cost_2024)
        END AS cogs_volume_effect,
        
        /* Effect 2: Freight Rate Effect - change in freight as % of COGS */
        CASE
            WHEN j.cogs_2024 < 10000 OR j.cogs_2025 < 10000 THEN
                (j.cogs_2025 * LEAST(j.freight_pct_cogs_2025, 0.50) - j.cogs_2025 * LEAST(j.freight_pct_cogs_2024, 0.50))
            ELSE
                (j.cogs_2025 * j.freight_pct_cogs_2025 - j.cogs_2025 * j.freight_pct_cogs_2024)
        END AS freight_rate_effect,
        
        /* Raw values (uncapped) for reference */
        (j.cogs_2025 * j.freight_pct_cogs_2024 - j.cost_2024) AS raw_cogs_volume_effect,
        (j.cogs_2025 * j.freight_pct_cogs_2025 - j.cogs_2025 * j.freight_pct_cogs_2024) AS raw_freight_rate_effect,
        
        /* % change in metrics */
        CASE 
            WHEN j.cogs_2024 = 0 THEN NULL 
            ELSE (j.cogs_2025 - j.cogs_2024) / j.cogs_2024 
        END AS cogs_pct_change,
        
        CASE 
            WHEN j.cost_2024 = 0 THEN NULL 
            ELSE (j.cost_2025 - j.cost_2024) / j.cost_2024 
        END AS cost_pct_change,
        
        CASE 
            WHEN j.trailers_2024 = 0 THEN NULL 
            ELSE (j.trailers_2025 - j.trailers_2024) / j.trailers_2024 
        END AS trailers_pct_change,
        
        CASE 
            WHEN j.cube_per_trailer_2024 = 0 THEN NULL 
            ELSE (j.cube_per_trailer_2025 - j.cube_per_trailer_2024) / j.cube_per_trailer_2024 
        END AS cube_per_trailer_pct_change
    FROM joined j
    JOIN lane_mix_calc lmc 
        ON j.lane = lmc.lane 
        AND j.fiscal_period = lmc.fiscal_period
)

SELECT
    lane,
    fiscal_period,
    go_live_flag,
    volume_confidence,
    cogs_2024,
    cogs_2025,
    cogs_pct_change,
    cost_2024,
    cost_2025,
    total_cost_diff,
    cost_pct_change,
    
    /* Lane mix metrics */
    lane_mix_pct_2024,
    lane_mix_pct_2025,
    lane_mix_effect,
    
    /* Rate and volume effects */
    freight_pct_cogs_2024,
    freight_pct_cogs_2025,
    ROUND(cogs_volume_effect, 2) AS cogs_volume_effect,
    ROUND(freight_rate_effect, 2) AS freight_rate_effect,
    
    /* Trailers and efficiency metrics */
    trailers_2024,
    trailers_2025,
    trailers_pct_change,
    ROUND(cost_per_trailer_2024, 2) AS cost_per_trailer_2024,
    ROUND(cost_per_trailer_2025, 2) AS cost_per_trailer_2025,
    ROUND(cube_per_trailer_2024, 2) AS cube_per_trailer_2024,
    ROUND(cube_per_trailer_2025, 2) AS cube_per_trailer_2025,
    
    /* Interpret the savings */
    CASE 
        WHEN total_cost_diff < 0 THEN 'Savings'
        WHEN total_cost_diff > 0 THEN 'Cost Increase'
        ELSE 'No Change'
    END AS cost_trend,
    
    CASE
        WHEN freight_pct_cogs_2025 < freight_pct_cogs_2024 THEN 'Improved Efficiency'
        WHEN freight_pct_cogs_2025 > freight_pct_cogs_2024 THEN 'Decreased Efficiency'
        ELSE 'No Change in Efficiency'
    END AS efficiency_trend
FROM waterfall
ORDER BY 
    fiscal_period,
    go_live_flag DESC, 
    volume_confidence DESC,
    ABS(total_cost_diff) DESC;


    --STEP 1 FULL LANE DATA
    SELECT * FROM DM_DECISION_ANALYTICS.BENEFIT_TRACKING.INTRANETWORK_LANE_DATA_BY_PERIOD_RESULTS;


    WITH period_summary AS (
    SELECT
        fiscal_period,
        SUM(CASE WHEN go_live_flag THEN cogs_2024 ELSE 0 END) AS golive_cogs_2024,
        SUM(CASE WHEN go_live_flag THEN cogs_2025 ELSE 0 END) AS golive_cogs_2025,
        SUM(CASE WHEN go_live_flag THEN cost_2024 ELSE 0 END) AS golive_cost_2024,
        SUM(CASE WHEN go_live_flag THEN cost_2025 ELSE 0 END) AS golive_cost_2025,
        
        SUM(CASE WHEN NOT go_live_flag THEN cogs_2024 ELSE 0 END) AS nongolive_cogs_2024,
        SUM(CASE WHEN NOT go_live_flag THEN cogs_2025 ELSE 0 END) AS nongolive_cogs_2025,
        SUM(CASE WHEN NOT go_live_flag THEN cost_2024 ELSE 0 END) AS nongolive_cost_2024,
        SUM(CASE WHEN NOT go_live_flag THEN cost_2025 ELSE 0 END) AS nongolive_cost_2025,
        
        SUM(CASE WHEN go_live_flag THEN cogs_volume_effect ELSE 0 END) AS golive_volume_effect,
        SUM(CASE WHEN go_live_flag THEN freight_rate_effect ELSE 0 END) AS golive_rate_effect,
        SUM(CASE WHEN go_live_flag THEN lane_mix_effect ELSE 0 END) AS golive_lane_mix_effect,
        
        SUM(CASE WHEN NOT go_live_flag THEN cogs_volume_effect ELSE 0 END) AS nongolive_volume_effect,
        SUM(CASE WHEN NOT go_live_flag THEN freight_rate_effect ELSE 0 END) AS nongolive_rate_effect,
        SUM(CASE WHEN NOT go_live_flag THEN lane_mix_effect ELSE 0 END) AS nongolive_lane_mix_effect
    FROM DM_DECISION_ANALYTICS.BENEFIT_TRACKING.INTRANETWORK_LANE_DATA_BY_PERIOD_RESULTS
    GROUP BY fiscal_period
)

SELECT top 10
    fiscal_period,
    golive_cogs_2024,
    golive_cogs_2025,
    (golive_cogs_2025 - golive_cogs_2024) AS golive_cogs_change,
    golive_cost_2024,
    golive_cost_2025,
    (golive_cost_2025 - golive_cost_2024) AS golive_cost_change,
    
    CASE WHEN golive_cogs_2024 = 0 THEN NULL
         ELSE golive_cost_2024 / golive_cogs_2024 END AS golive_freight_pct_2024,
    
    CASE WHEN golive_cogs_2025 = 0 THEN NULL
         ELSE golive_cost_2025 / golive_cogs_2025 END AS golive_freight_pct_2025,
    
    golive_volume_effect,
    golive_rate_effect,
    golive_lane_mix_effect,
    
    nongolive_cogs_2024,
    nongolive_cogs_2025,
    (nongolive_cogs_2025 - nongolive_cogs_2024) AS nongolive_cogs_change,
    nongolive_cost_2024,
    nongolive_cost_2025,
    (nongolive_cost_2025 - nongolive_cost_2024) AS nongolive_cost_change,
    
    CASE WHEN nongolive_cogs_2024 = 0 THEN NULL
         ELSE nongolive_cost_2024 / nongolive_cogs_2024 END AS nongolive_freight_pct_2024,
    
    CASE WHEN nongolive_cogs_2025 = 0 THEN NULL
         ELSE nongolive_cost_2025 / nongolive_cogs_2025 END AS nongolive_freight_pct_2025,
    
    nongolive_volume_effect,
    nongolive_rate_effect,
    nongolive_lane_mix_effect
FROM period_summary
ORDER BY fiscal_period;

Select * 
FROM DM_DECISION_ANALYTICS.BENEFIT_TRACKING.INTRANETWORK_LANE_DATA_BY_PERIOD_RESULTS;


// Truncate files
// STO
Select * from DM_DECISION_ANALYTICS.AD_HOC."2025_CARRIER_INVOICE_STO";
truncate DM_DECISION_ANALYTICS.AD_HOC."2025_CARRIER_INVOICE_STO";
truncate DM_DECISION_ANALYTICS.AD_HOC.EDI_Transfers_STO;
truncate DM_DECISION_ANALYTICS.AD_HOC.STO_ACTIVITY;

// LH
truncate DM_DECISION_ANALYTICS.AD_HOC."2025_Carrier_Invoice";
truncate DM_DECISION_ANALYTICS.AD_HOC."2025_Activity" ;


/// STEP 1 PEDRO
CREATE OR REPLACE TABLE DM_DECISION_ANALYTICS.BENEFIT_TRACKING.STO_ACTIVITY AS (
WITH INV_CTE AS (
  SELECT 
    '2025_CARRIER_INVOICE_STO' AS source_table,
    BOL, 
    PO_Number, 
    Ship_Period, 
    Ship_Week, 
    Ship_Year, 
    BU,
    Origin_Name, 
    Origin_Code, 
    Origin_CC, 
    Origin_Reporting_DC_Name,
    Dest_Name, 
    Dest_Code, 
    Dest_CC, 
    Dest_DC_Name,
    Settlement_Total, 
    Create_Date, 
    Actual_Ship, 
    Actual_Ship_2,
    PRO_Number, 
    Origin_Zip, 
    Dest_Zip,
    Divison_, 
    Region, 
    Network,
    CARRIER_MODE,
    --check for nulls.
    CASE
      WHEN BU = 'HDS' AND GL_Account_Match = 'Transfer' AND PO_Number NOT ILIKE '%RTN%' AND PO_Number NOT ILIKE '%yard%' AND PO_Number NOT ILIKE '%shuttle%' THEN 'HDS TRANSFER'
      WHEN BU = 'HDP' AND GL_Account_Match = 'Transfer' AND PO_Number NOT ILIKE '%RTN%' AND PO_Number NOT ILIKE '%yard%' AND PO_Number NOT ILIKE '%shuttle%' THEN 'HDP TRANSFER'
      WHEN GL_Account_Match = 'Transfer' AND (PO_Number ILIKE '%yard%' OR PO_Number ILIKE '%shuttle%') THEN 'Yard/ Shuttle Moves'
      WHEN GL_Account_Match = 'Transfer' AND PO_Number ILIKE '%RTN%' THEN 'Return Trailer'
      ELSE NULL
    END AS STO_EXPENSE,

    CASE
      WHEN BU = 'HDS' AND GL_Account_Match = 'Transfer' AND Invoice_SCAC NOT ILIKE '%BD%' AND PO_Number NOT ILIKE '%RTN%' AND PO_Number NOT ILIKE '%yard%' AND PO_Number NOT ILIKE '%shuttle%' THEN 1
      ELSE 0
    END AS HDS_TRANSFER_TRL,

    CASE
      WHEN BU = 'HDP' AND GL_Account_Match = 'Transfer' AND Invoice_SCAC NOT ILIKE '%BD%' AND PO_Number NOT ILIKE '%RTN%' AND PO_Number NOT ILIKE '%yard%' AND PO_Number NOT ILIKE '%shuttle%' THEN 1
      ELSE 0
    END AS HDP_TRANSFER_TRL,

    CASE
      WHEN GL_Account_Match = 'Transfer' AND (PO_Number ILIKE '%yard%' OR PO_Number ILIKE '%shuttle%') THEN 1
      ELSE 0
    END AS YARD_SHUTTLE_MOVES_TRL,

    CASE
      WHEN GL_Account_Match = 'Transfer' AND PO_Number ILIKE '%RTN%' THEN 1
      ELSE 0
    END AS RETURN_TRL,

    CASE 
        WHEN STO_EXPENSE IS NOT NULL
    THEN 1 
    ELSE 0 
    END AS STO_TRL,

    CASE 
        WHEN STO_EXPENSE IS NOT NULL
    THEN 'STO' 
    ELSE NULL
    END AS MODE_GROUP,

    --ORIGIN--
ORG.XD_FLAG AS ORIGIN_XD_FLAG,
ORG.C2 AS ORIGIN_PLANT_NAME, 

  CASE 
    WHEN ORG.C2 LIKE 'PRO%' THEN
      SUBSTRING(ORG.C2, 4, POSITION('-' IN ORG.C2) - 4)
    ELSE
      SPLIT_PART(ORG.C2, '-', 1)
      END AS ORIGIN_ID_ORIGINAL,

IFNULL(ORG.STO_TRANSACTIONAL_ID,ORIGIN_ID_ORIGINAL) AS ORIGIN_PLANT_ID, --USED FOR LANE


--DESTINATION--
DEST.XD_FLAG AS DEST_XD_FLAG,
DEST.C2_ORG AS DEST_SOURCE_BUSINESS,
DEST.C2 AS DEST_PLANT_NAME,

  CASE 
    WHEN DEST.C2 LIKE 'PRO%' THEN
      SUBSTRING(DEST.C2, 4, POSITION('-' IN DEST.C2) - 4)
    ELSE
      SPLIT_PART(DEST.C2, '-', 1)
      END AS DESTINATION_ID_ORIGINAL,

IFNULL(CASE WHEN MODE_GROUP = 'LH' --USING LOGIC ABOVE FOR MODE_GROUP
    THEN DEST.LH_TRANSACTIONAL_ID
    ELSE DEST.STO_TRANSACTIONAL_ID
        END,
        DESTINATION_ID_ORIGINAL)
        AS DEST_PLANT_ID, --USED FOR LANE

CONCAT(ORIGIN_PLANT_ID,'-',DEST_PLANT_ID) AS SHIP_LANE
    

  FROM DM_DECISION_ANALYTICS.AD_HOC."2025_CARRIER_INVOICE_STO" INV
    LEFT JOIN DM_DECISION_ANALYTICS.BENEFIT_TRACKING.DC_DIM_AL ORG
        ON ORG.C1 = INV.ORIGIN_NAME
        AND ORG.ZIP_5 = SPLIT_PART(INV.ORIGIN_ZIP, '-', 1)
    LEFT JOIN DM_DECISION_ANALYTICS.BENEFIT_TRACKING.DC_DIM_AL DEST
        ON DEST.C1 = INV.DEST_NAME
        AND DEST.ZIP_5 = SPLIT_PART(INV.DEST_ZIP, '-', 1)

WHERE SHIP_YEAR = '2025'

GROUP BY ALL

  UNION ALL

  SELECT 
    '2024_CARRIER_INVOICE_STO' AS source_table,
    BOL, 
    PO_Number, 
    Ship_Period, 
    Ship_Week, 
    Ship_Year, 
    BU,
    Origin_Name, 
    Origin_Code, 
    Origin_CC, 
    Origin_Reporting_DC_Name,
    Dest_Name, 
    Dest_Code, 
    Dest_CC, 
    Dest_DC_Name,
    Settlement_Total, 
    Create_Date, 
    Actual_Ship, 
    Actual_Ship_2,
    PRO_Number, 
    Origin_Zip, 
    Dest_Zip,
    Divison_, 
    Region, 
    Network,
    CARRIER_MODE,

    CASE
      WHEN BU = 'HDS' AND GL_Account_Match = 'Transfer' AND PO_Number NOT ILIKE '%RTN%' AND PO_Number NOT ILIKE '%yard%' AND PO_Number NOT ILIKE '%shuttle%' THEN 'HDS TRANSFER'
      WHEN BU = 'HDP' AND GL_Account_Match = 'Transfer' AND PO_Number NOT ILIKE '%RTN%' AND PO_Number NOT ILIKE '%yard%' AND PO_Number NOT ILIKE '%shuttle%' THEN 'HDP TRANSFER'
      WHEN GL_Account_Match = 'Transfer' AND (PO_Number ILIKE '%yard%' OR PO_Number ILIKE '%shuttle%') THEN 'Yard/ Shuttle Moves'
      WHEN GL_Account_Match = 'Transfer' AND PO_Number ILIKE '%RTN%' THEN 'Return Trailer'
      ELSE NULL
    END AS STO_EXPENSE,

    CASE
      WHEN BU = 'HDS' AND GL_Account_Match = 'Transfer' AND PRO_NUMBER NOT ILIKE '%BD%' AND PO_Number NOT ILIKE '%RTN%' AND PO_Number NOT ILIKE '%yard%' AND PO_Number NOT ILIKE '%shuttle%' THEN 1
      ELSE 0
    END AS HDS_TRANSFER_TRL,

    CASE
      WHEN BU = 'HDP' AND GL_Account_Match = 'Transfer' AND PRO_NUMBER NOT ILIKE '%BD%' AND PO_Number NOT ILIKE '%RTN%' AND PO_Number NOT ILIKE '%yard%' AND PO_Number NOT ILIKE '%shuttle%' THEN 1
      ELSE 0
    END AS HDP_TRANSFER_TRL,

    CASE
      WHEN GL_Account_Match = 'Transfer' AND PRO_NUMBER NOT ILIKE '%BD%' AND (PO_Number ILIKE '%yard%' OR PO_Number ILIKE '%shuttle%') THEN 1
      ELSE 0
    END AS YARD_SHUTTLE_MOVES_TRL,

    CASE
      WHEN GL_Account_Match = 'Transfer' AND PRO_NUMBER NOT ILIKE '%BD%' AND PO_Number ILIKE '%RTN%' THEN 1
      ELSE 0
    END AS RETURN_TRL,

    CASE 
        WHEN STO_EXPENSE IS NOT NULL
    THEN 1 
    ELSE 0 
    END AS STO_TRL,

    CASE 
        WHEN STO_EXPENSE IS NOT NULL
    THEN 'STO' 
    ELSE NULL
    END AS MODE_GROUP,

    --ORIGIN--
ORG.XD_FLAG AS ORIGIN_XD_FLAG,
ORG.C2 AS ORIGIN_PLANT_NAME, 

  CASE 
    WHEN ORG.C2 LIKE 'PRO%' THEN
      SUBSTRING(ORG.C2, 4, POSITION('-' IN ORG.C2) - 4)
    ELSE
      SPLIT_PART(ORG.C2, '-', 1)
      END AS ORIGIN_ID_ORIGINAL,

IFNULL(ORG.STO_TRANSACTIONAL_ID,ORIGIN_ID_ORIGINAL) AS ORIGIN_PLANT_ID, --USED FOR LANE


--DESTINATION--
DEST.XD_FLAG AS DEST_XD_FLAG,
DEST.C2_ORG AS DEST_SOURCE_BUSINESS,
DEST.C2 AS DEST_PLANT_NAME,

  CASE 
    WHEN DEST.C2 LIKE 'PRO%' THEN
      SUBSTRING(DEST.C2, 4, POSITION('-' IN DEST.C2) - 4)
    ELSE
      SPLIT_PART(DEST.C2, '-', 1)
      END AS DESTINATION_ID_ORIGINAL,

IFNULL(CASE WHEN MODE_GROUP = 'LH' --USING LOGIC ABOVE FOR MODE_GROUP
    THEN DEST.LH_TRANSACTIONAL_ID
    ELSE DEST.STO_TRANSACTIONAL_ID
        END,
        DESTINATION_ID_ORIGINAL)
        AS DEST_PLANT_ID, --USED FOR LANE

CONCAT(ORIGIN_PLANT_ID,'-',DEST_PLANT_ID) AS SHIP_LANE

  FROM DM_DECISION_ANALYTICS.AD_HOC."2024_CARRIER_INVOICE_STO" INV
    LEFT JOIN DM_DECISION_ANALYTICS.BENEFIT_TRACKING.DC_DIM_AL ORG
        ON ORG.C1 = INV.ORIGIN_NAME
        AND ORG.ZIP_5 = SPLIT_PART(INV.ORIGIN_ZIP, '-', 1)
    LEFT JOIN DM_DECISION_ANALYTICS.BENEFIT_TRACKING.DC_DIM_AL DEST
        ON DEST.C1 = INV.DEST_NAME
        AND DEST.ZIP_5 = SPLIT_PART(INV.DEST_ZIP, '-', 1)

      WHERE GL_Account_Match IN ('Transfer', 'Shuttle')
      AND SHIP_YEAR = '2024'
      
GROUP BY ALL

),

EDI_CTE AS (
  SELECT
    'EDI_TRANSFERS_STO' AS source_table,
    BOL, 
    PO_Number, 
    Ship_Period, 
    Ship_Week, 
    Ship_Year, 
    BU,
    Origin_Name, 
    Origin_Code, 
    Origin_CC, 
    Origin_Reporting_DC_Name,
    Dest_Name, 
    Dest_Code, 
    Dest_CC, 
    Dest_DC_Name,
    Settlement_Total, 
    Create_Date, 
    Actual_Ship, 
    Actual_Ship_2,
    PRO_Number, 
    Origin_Zip, 
    Dest_Zip,
    Divison_, 
    Region, 
    Network,
    CARRIER_MODE,

    CASE
      WHEN BU = 'HDS' AND GL_Account_Match = 'Transfer' AND Carrier_Mode <> 'Small Package' THEN 'HDS TRANSFER'
      WHEN BU = 'HDP' AND GL_Account_Match = 'Transfer' AND Carrier_Mode <> 'Small Package' THEN 'HDP TRANSFER'
      WHEN GL_Account_Match = 'Shuttle' AND Invoice_SCAC NOT IN ('ACXM', 'NRLG') THEN 'Yard/ Shuttle Moves'
      ELSE NULL
    END AS STO_EXPENSE,

    CASE
      WHEN BU = 'HDS' AND GL_Account_Match = 'Transfer' AND Carrier_Mode <> 'Small Package' AND Invoice_SCAC NOT ILIKE '%BD%' THEN 1
      ELSE 0
    END AS HDS_TRANSFER_TRL,

    CASE
      WHEN BU = 'HDP' AND GL_Account_Match = 'Transfer' AND Carrier_Mode <> 'Small Package' AND Invoice_SCAC NOT ILIKE '%BD%' THEN 1
      ELSE 0
    END AS HDP_TRANSFER_TRL,

    CASE
      WHEN GL_Account_Match = 'Shuttle' AND Invoice_SCAC NOT IN ('ACXM', 'NRLG') THEN 1
      ELSE 0
    END AS YARD_SHUTTLE_MOVES_TRL,

    0 AS RETURN_TRL,

    CASE 
        WHEN STO_EXPENSE IS NOT NULL
    THEN 1 
    ELSE 0 
    END AS STO_TRL,

    CASE 
        WHEN STO_EXPENSE IS NOT NULL
    THEN 'STO' 
    ELSE NULL
    END AS MODE_GROUP,

    --ORIGIN--
ORG.XD_FLAG AS ORIGIN_XD_FLAG,
ORG.C2 AS ORIGIN_PLANT_NAME, 

  CASE 
    WHEN ORG.C2 LIKE 'PRO%' THEN
      SUBSTRING(ORG.C2, 4, POSITION('-' IN ORG.C2) - 4)
    ELSE
      SPLIT_PART(ORG.C2, '-', 1)
      END AS ORIGIN_ID_ORIGINAL,

IFNULL(ORG.STO_TRANSACTIONAL_ID,ORIGIN_ID_ORIGINAL) AS ORIGIN_PLANT_ID, --USED FOR LANE


--DESTINATION--
DEST.XD_FLAG AS DEST_XD_FLAG,
DEST.C2_ORG AS DEST_SOURCE_BUSINESS,
DEST.C2 AS DEST_PLANT_NAME,

  CASE 
    WHEN DEST.C2 LIKE 'PRO%' THEN
      SUBSTRING(DEST.C2, 4, POSITION('-' IN DEST.C2) - 4)
    ELSE
      SPLIT_PART(DEST.C2, '-', 1)
      END AS DESTINATION_ID_ORIGINAL,

IFNULL(CASE WHEN MODE_GROUP = 'LH' --USING LOGIC ABOVE FOR MODE_GROUP
    THEN DEST.LH_TRANSACTIONAL_ID
    ELSE DEST.STO_TRANSACTIONAL_ID
        END,
        DESTINATION_ID_ORIGINAL)
        AS DEST_PLANT_ID, --USED FOR LANE

CONCAT(ORIGIN_PLANT_ID,'-',DEST_PLANT_ID) AS SHIP_LANE

  FROM DM_DECISION_ANALYTICS.AD_HOC.EDI_Transfers_STO EDI
    LEFT JOIN DM_DECISION_ANALYTICS.BENEFIT_TRACKING.DC_DIM_AL ORG
        ON ORG.C1 = EDI.ORIGIN_NAME
        AND ORG.ZIP_5 = SPLIT_PART(EDI.ORIGIN_ZIP, '-', 1)
    LEFT JOIN DM_DECISION_ANALYTICS.BENEFIT_TRACKING.DC_DIM_AL DEST
        ON DEST.C1 = EDI.DEST_NAME
        AND DEST.ZIP_5 = SPLIT_PART(EDI.DEST_ZIP, '-', 1)

WHERE SHIP_YEAR = '2025'

GROUP BY ALL

  UNION ALL

  SELECT
    '2024_EDI_TRANSFERS_STO' AS source_table,
    BOL, 
    PO_Number, 
    Ship_Period, 
    Ship_Week, 
    Ship_Year, 
    BU,
    Origin_Name, 
    Origin_Code, 
    Origin_CC, 
    Origin_Reporting_DC_Name,
    Dest_Name, 
    Dest_Code, 
    Dest_CC, 
    Dest_DC_Name,
    Settlement_Total, 
    Create_Date, 
    Actual_Ship, 
    Actual_Ship_2,
    PRO_Number, 
    Origin_Zip, 
    Dest_Zip,
    Divison_, 
    Region, 
    Network,
    CARRIER_MODE,

    CASE
      WHEN BU = 'HDS' AND GL_Account_Match = 'Transfer' AND Carrier_Mode <> 'Small Package' THEN 'HDS TRANSFER'
      WHEN BU = 'HDP' AND GL_Account_Match = 'Transfer' AND Carrier_Mode <> 'Small Package' THEN 'HDP TRANSFER'
      WHEN GL_Account_Match = 'Shuttle' AND Invoice_SCAC NOT IN ('ACXM', 'NRLG') THEN 'Yard/ Shuttle Moves'
      ELSE NULL
    END AS STO_EXPENSE,

    CASE
      WHEN BU = 'HDS' AND GL_Account_Match = 'Transfer' AND Carrier_Mode <> 'Small Package' AND PRO_NUMBER NOT ILIKE '%BD%' THEN 1
      ELSE 0
    END AS HDS_TRANSFER_TRL,

    CASE
      WHEN BU = 'HDP' AND GL_Account_Match = 'Transfer' AND Carrier_Mode <> 'Small Package' AND PRO_NUMBER NOT ILIKE '%BD%' THEN 1
      ELSE 0
    END AS HDP_TRANSFER_TRL,

    CASE
      WHEN GL_Account_Match = 'Shuttle' AND PRO_NUMBER NOT ILIKE '%BD%' AND Invoice_SCAC NOT IN ('ACXM', 'NRLG') THEN 1
      ELSE 0
    END AS YARD_SHUTTLE_MOVES_TRL,

    0 AS RETURN_TRL, 

    CASE 
        WHEN STO_EXPENSE IS NOT NULL
    THEN 1 
    ELSE 0 
    END AS STO_TRL,

    CASE 
        WHEN STO_EXPENSE IS NOT NULL
    THEN 'STO' 
    ELSE NULL
    END AS MODE_GROUP,

    --ORIGIN--
ORG.XD_FLAG AS ORIGIN_XD_FLAG,
ORG.C2 AS ORIGIN_PLANT_NAME, 

  CASE 
    WHEN ORG.C2 LIKE 'PRO%' THEN
      SUBSTRING(ORG.C2, 4, POSITION('-' IN ORG.C2) - 4)
    ELSE
      SPLIT_PART(ORG.C2, '-', 1)
      END AS ORIGIN_ID_ORIGINAL,

IFNULL(ORG.STO_TRANSACTIONAL_ID,ORIGIN_ID_ORIGINAL) AS ORIGIN_PLANT_ID, --USED FOR LANE


--DESTINATION--
DEST.XD_FLAG AS DEST_XD_FLAG,
DEST.C2_ORG AS DEST_SOURCE_BUSINESS,
DEST.C2 AS DEST_PLANT_NAME,

  CASE 
    WHEN DEST.C2 LIKE 'PRO%' THEN
      SUBSTRING(DEST.C2, 4, POSITION('-' IN DEST.C2) - 4)
    ELSE
      SPLIT_PART(DEST.C2, '-', 1)
      END AS DESTINATION_ID_ORIGINAL,

IFNULL(CASE WHEN MODE_GROUP = 'LH' --USING LOGIC ABOVE FOR MODE_GROUP
    THEN DEST.LH_TRANSACTIONAL_ID
    ELSE DEST.STO_TRANSACTIONAL_ID
        END,
        DESTINATION_ID_ORIGINAL)
        AS DEST_PLANT_ID, --USED FOR LANE

CONCAT(ORIGIN_PLANT_ID,'-',DEST_PLANT_ID) AS SHIP_LANE

  FROM DM_DECISION_ANALYTICS.AD_HOC."2024_EDI_TRANSFERS_STO" EDI
    LEFT JOIN DM_DECISION_ANALYTICS.BENEFIT_TRACKING.DC_DIM_AL ORG
        ON ORG.C1 = EDI.ORIGIN_NAME
        AND ORG.ZIP_5 = SPLIT_PART(EDI.ORIGIN_ZIP, '-', 1)
    LEFT JOIN DM_DECISION_ANALYTICS.BENEFIT_TRACKING.DC_DIM_AL DEST
        ON DEST.C1 = EDI.DEST_NAME
        AND DEST.ZIP_5 = SPLIT_PART(EDI.DEST_ZIP, '-', 1)

  WHERE GL_Account_Match IN ('Transfer', 'Shuttle')
    AND (
      GL_Account_Match <> 'Shuttle'
      OR Invoice_SCAC NOT IN ('NRLG', 'CCGY', 'ACXM'))
    AND SHIP_YEAR = '2024'

GROUP BY ALL 

),

ACT_CTE AS (
  SELECT 
    'STO_ACTIVITY' AS source_table,
    BOL, 
    PO_Number, 
    Ship_Period, 
    Ship_Week, 
    Ship_Year, 
    BU,
    Origin_Name, 
    Origin_Code, 
    Origin_CC, 
    Origin_Reporting_DC_Name,
    Dest_Name, 
    Dest_Code, 
    Dest_CC, 
    Dest_DC_Name,
    Carrier_Charge AS Settlement_Total, 
    Create_Date, 
    Actual_Ship,
    Actual_Ship_2,
    PRO_Number, 
    ORIGIN_ZIP AS Origin_Zip, 
    DEST_ZIP AS Dest_Zip,
    Divison_, 
    Region, 
    Network,
    NULL AS CARRIER_MODE,

    CASE
      WHEN BU = 'HDS' AND Match = 'No Match' AND PO_Number NOT ILIKE '%RTN%' AND PO_Number NOT ILIKE '%yard%' THEN 'HDS TRANSFER'
      WHEN BU = 'HDP' AND Match = 'No Match' AND PO_Number NOT ILIKE '%RTN%' AND PO_Number NOT ILIKE '%yard%' THEN 'HDP TRANSFER'
      WHEN Match = 'No Match' AND PO_Number ILIKE '%yard%' AND PO_Number NOT ILIKE '%RTN%' THEN 'Yard/ Shuttle Moves'
      WHEN Match = 'No Match' AND PO_Number ILIKE '%RTN%' AND PO_Number NOT ILIKE '%yard%' THEN 'Return Trailer'
      ELSE NULL
    END AS STO_EXPENSE,

    CASE
      WHEN BU = 'HDS' AND Match = 'No Match' AND PO_Number NOT ILIKE '%RTN%' AND PO_Number NOT ILIKE '%yard%' THEN 1
      ELSE 0
    END AS HDS_TRANSFER_TRL,

    CASE
      WHEN BU = 'HDP' AND Match = 'No Match' AND PO_Number NOT ILIKE '%RTN%' AND PO_Number NOT ILIKE '%yard%' THEN 1
      ELSE 0
    END AS HDP_TRANSFER_TRL,

    CASE
      WHEN Match = 'No Match' AND PO_Number ILIKE '%yard%' AND PO_Number NOT ILIKE '%RTN%' THEN 1
      ELSE 0
    END AS YARD_SHUTTLE_MOVES_TRL,

    CASE
      WHEN Match = 'No Match' AND PO_Number ILIKE '%RTN%' AND PO_Number NOT ILIKE '%yard%' THEN 1
      ELSE 0
    END AS RETURN_TRL,

    CASE 
        WHEN STO_EXPENSE IS NOT NULL
    THEN 1 
    ELSE 0 
    END AS STO_TRL,

    CASE 
        WHEN STO_EXPENSE IS NOT NULL
    THEN 'STO' 
    ELSE NULL
    END AS MODE_GROUP,

    --ORIGIN--
ORG.XD_FLAG AS ORIGIN_XD_FLAG,
ORG.C2 AS ORIGIN_PLANT_NAME, 

  CASE 
    WHEN ORG.C2 LIKE 'PRO%' THEN
      SUBSTRING(ORG.C2, 4, POSITION('-' IN ORG.C2) - 4)
    ELSE
      SPLIT_PART(ORG.C2, '-', 1)
      END AS ORIGIN_ID_ORIGINAL,

IFNULL(ORG.STO_TRANSACTIONAL_ID,ORIGIN_ID_ORIGINAL) AS ORIGIN_PLANT_ID, --USED FOR LANE


--DESTINATION--
DEST.XD_FLAG AS DEST_XD_FLAG,
DEST.C2_ORG AS DEST_SOURCE_BUSINESS,
DEST.C2 AS DEST_PLANT_NAME,

  CASE 
    WHEN DEST.C2 LIKE 'PRO%' THEN
      SUBSTRING(DEST.C2, 4, POSITION('-' IN DEST.C2) - 4)
    ELSE
      SPLIT_PART(DEST.C2, '-', 1)
      END AS DESTINATION_ID_ORIGINAL,

IFNULL(CASE WHEN MODE_GROUP = 'LH' --USING LOGIC ABOVE FOR MODE_GROUP
    THEN DEST.LH_TRANSACTIONAL_ID
    ELSE DEST.STO_TRANSACTIONAL_ID
        END,
        DESTINATION_ID_ORIGINAL)
        AS DEST_PLANT_ID, --USED FOR LANE

CONCAT(ORIGIN_PLANT_ID,'-',DEST_PLANT_ID) AS SHIP_LANE

  FROM DM_DECISION_ANALYTICS.AD_HOC.STO_ACTIVITY ACT
    LEFT JOIN DM_DECISION_ANALYTICS.BENEFIT_TRACKING.DC_DIM_AL ORG
        ON ORG.C1 = ACT.ORIGIN_NAME
        AND ORG.ZIP_5 = SPLIT_PART(ACT.ORIGIN_ZIP, '-', 1)
    LEFT JOIN DM_DECISION_ANALYTICS.BENEFIT_TRACKING.DC_DIM_AL DEST
        ON DEST.C1 = ACT.DEST_NAME
        AND DEST.ZIP_5 = SPLIT_PART(ACT.DEST_ZIP, '-', 1)

WHERE SHIP_YEAR = '2025'

GROUP BY ALL
  
)


SELECT * FROM INV_CTE
UNION ALL
SELECT * FROM EDI_CTE
UNION ALL
SELECT * FROM ACT_CTE

);

select * from DM_DECISION_ANALYTICS.BENEFIT_TRACKING.STO_ACTIVITY;


// STEP 2 PEDRO

CREATE OR REPLACE TABLE DM_DECISION_ANALYTICS.BENEFIT_TRACKING.LINEHAUL_ACTIVITY AS (
WITH carrier_2025 AS (
  SELECT
    CREATE_DATE,
    INVOICE_DATE,
    ACTUAL_SHIP_2,
    ACTUAL_SHIP,
    BOL,
    PRO_NUMBER,
    INVOICE_SCAC,
    ORIGIN_NAME,
    ORIGIN_ZIP,
    ORIGIN_CODE,
    DEST_NAME,
    DEST_ZIP,
    DEST_CODE,
    SETTLEMENT_TOTAL,
    INVOICE_NUMBER,
    INVOICE_GL,
    EXTRACT_DATE,
    EXTRACT_DATE_2,
    PO_NUMBER,
    INVOICE_WEIGHT,
    OWNER,
    CARRIER_MODE,
    GL_ACCOUNT_MATCH,
    INVOICE_FISCAL_PERIOD,
    ORIGIN_CC,
    ORIGIN_DC_NAME,
    REPORTING_DC_NAME,
    REPORTING_CC,
    DIVISON_,
    REGION,
    NETWORK,
    BU,
    EXTRACT_PERIOD,
    SHIP_PERIOD,
    SHIP_WEEK,
    SHIP_YEAR,
    -- LH_EXPENSE logic
    CASE
      WHEN BOL NOT ILIKE '%Cancelled%'
           AND PO_NUMBER NOT ILIKE '%Overflow%'
           AND PO_NUMBER NOT ILIKE '%RTN%'
           AND SHIP_YEAR <> '2024'
        THEN 'Contract'
      WHEN BOL NOT ILIKE '%Cancelled%'
           AND PO_NUMBER ILIKE '%Overflow%'
           AND SHIP_YEAR <> '2024'
        THEN 'Overflow'
      WHEN BOL NOT ILIKE '%Cancelled%'
           AND PO_NUMBER ILIKE '%RTN%'
           AND SHIP_YEAR <> '2024'
        THEN 'Return'
      ELSE NULL
    END AS LH_EXPENSE,
    -- _TRL flags
    CASE
      WHEN PRO_NUMBER NOT ILIKE '%BD%'
           AND BOL NOT ILIKE '%Cancelled%'
           AND PO_NUMBER NOT ILIKE '%Overflow%'
           AND PO_NUMBER NOT ILIKE '%RTN%' THEN 1 ELSE 0
    END AS CONTRACT_TRL,
    CASE
      WHEN SHIP_YEAR <> '2024'
           AND PRO_NUMBER NOT ILIKE '%BD%'
           AND BOL NOT ILIKE '%Cancelled%'
           AND PO_NUMBER ILIKE '%Overflow%' THEN 1 ELSE 0
    END AS OVERFLOW_TRL,
    CASE
      WHEN PRO_NUMBER NOT ILIKE '%BD%'
           AND BOL NOT ILIKE '%Cancelled%'
           AND PO_NUMBER ILIKE '%RTN%' THEN 1 ELSE 0
    END AS RETURN_TRL,
    CASE
      WHEN BOL NOT ILIKE '%Cancelled%'
           AND (PO_NUMBER ILIKE '%Overflow%' OR PO_NUMBER ILIKE '%RTN%' 
                OR (PO_NUMBER NOT ILIKE '%Overflow%' AND PO_NUMBER NOT ILIKE '%RTN%'))
      THEN 1 ELSE 0
    END AS LH_TRL,
    '2025_CARRIER_INVOICE' AS DATA_SOURCE,


    CASE 
        WHEN LH_EXPENSE IS NOT NULL
    THEN 'LH' 
    ELSE NULL
    END AS MODE_GROUP,

    --ORIGIN--
ORG.XD_FLAG AS ORIGIN_XD_FLAG,
ORG.C2 AS ORIGIN_PLANT_NAME, 

  CASE 
    WHEN ORG.C2 LIKE 'PRO%' THEN
      SUBSTRING(ORG.C2, 4, POSITION('-' IN ORG.C2) - 4)
    ELSE
      SPLIT_PART(ORG.C2, '-', 1)
      END AS ORIGIN_ID_ORIGINAL,

IFNULL(ORG.STO_TRANSACTIONAL_ID,ORIGIN_ID_ORIGINAL) AS ORIGIN_PLANT_ID, --USED FOR LANE


--DESTINATION--
DEST.XD_FLAG AS DEST_XD_FLAG,
DEST.C2_ORG AS DEST_SOURCE_BUSINESS,
DEST.C2 AS DEST_PLANT_NAME,

  CASE 
    WHEN DEST.C2 LIKE 'PRO%' THEN
      SUBSTRING(DEST.C2, 4, POSITION('-' IN DEST.C2) - 4)
    ELSE
      SPLIT_PART(DEST.C2, '-', 1)
      END AS DESTINATION_ID_ORIGINAL,

IFNULL(CASE WHEN MODE_GROUP = 'LH' --USING LOGIC ABOVE FOR MODE_GROUP
    THEN DEST.LH_TRANSACTIONAL_ID
    ELSE DEST.STO_TRANSACTIONAL_ID
        END,
        DESTINATION_ID_ORIGINAL)
        AS DEST_PLANT_ID, --USED FOR LANE

CONCAT(ORIGIN_PLANT_ID,'-',DEST_PLANT_ID) AS SHIP_LANE

  FROM DM_DECISION_ANALYTICS.AD_HOC."2025_Carrier_Invoice" INV
    LEFT JOIN DM_DECISION_ANALYTICS.BENEFIT_TRACKING.DC_DIM_AL ORG
        ON ORG.C1 = INV.ORIGIN_NAME
        AND ORG.ZIP_5 = SPLIT_PART(INV.ORIGIN_ZIP, '-', 1)
    LEFT JOIN DM_DECISION_ANALYTICS.BENEFIT_TRACKING.DC_DIM_AL DEST
        ON DEST.C1 = INV.DEST_NAME
        AND DEST.ZIP_5 = SPLIT_PART(INV.DEST_ZIP, '-', 1)

  
  WHERE BU IN ('HDP','HDS')
),

carrier_2024 AS (
  SELECT
    CREATE_DATE,
    INVOICE_DATE,
    ACTUAL_SHIP_2,
    ACTUAL_SHIP,
    BOL,
    PRO_NUMBER,
    INVOICE_SCAC,
    ORIGIN_NAME,
    ORIGIN_ZIP,
    ORIGIN_CODE,
    DEST_NAME,
    DEST_ZIP,
    DEST_CODE,
    SETTLEMENT_TOTAL,
    INVOICE_NUMBER,
    INVOICE_GL,
    EXTRACT_DATE,
    EXTRACT_DATE_2,
    PO_NUMBER,
    INVOICE_WEIGHT,
    OWNER,
    CARRIER_MODE,
    GL_ACCOUNT_MATCH,
    INVOICE_FISCAL_PERIOD,
    ORIGIN_CC,
    ORIGIN_DC_NAME,
    REPORTING_DC_NAME,
    REPORTING_CC,
    DIVISON_,
    REGION,
    NETWORK,
    BU,
    EXTRACT_PERIOD,
    SHIP_PERIOD,
    SHIP_WEEK,
    SHIP_YEAR,
    -- LH_EXPENSE logic
    CASE
      WHEN BOL NOT ILIKE '%Cancelled%'
           AND PO_NUMBER NOT ILIKE '%Overflow%'
           AND PO_NUMBER NOT ILIKE '%RTN%'
           AND SHIP_YEAR = '2024'
        THEN 'Contract'
      WHEN BOL NOT ILIKE '%Cancelled%'
           AND PO_NUMBER ILIKE '%Overflow%'
           AND SHIP_YEAR = '2024'
        THEN 'Overflow'
      WHEN BOL NOT ILIKE '%Cancelled%'
           AND PO_NUMBER ILIKE '%RTN%'
           AND SHIP_YEAR = '2024'
        THEN 'Return'
      ELSE NULL
    END AS LH_EXPENSE,
    -- _TRL flags
    0 AS CONTRACT_TRL,
    0 AS OVERFLOW_TRL,
    0 AS RETURN_TRL,
    CASE
      WHEN BOL NOT ILIKE '%Cancelled%'
           AND (PO_NUMBER ILIKE '%Overflow%' OR PO_NUMBER ILIKE '%RTN%' 
                OR (PO_NUMBER NOT ILIKE '%Overflow%' AND PO_NUMBER NOT ILIKE '%RTN%'))
      THEN 1 ELSE 0
    END AS LH_TRL,
    '2024_CARRIER_INVOICE' AS DATA_SOURCE,

    CASE 
        WHEN LH_EXPENSE IS NOT NULL
    THEN 'LH' 
    ELSE NULL
    END AS MODE_GROUP,

    --ORIGIN--
ORG.XD_FLAG AS ORIGIN_XD_FLAG,
ORG.C2 AS ORIGIN_PLANT_NAME, 

  CASE 
    WHEN ORG.C2 LIKE 'PRO%' THEN
      SUBSTRING(ORG.C2, 4, POSITION('-' IN ORG.C2) - 4)
    ELSE
      SPLIT_PART(ORG.C2, '-', 1)
      END AS ORIGIN_ID_ORIGINAL,

IFNULL(ORG.STO_TRANSACTIONAL_ID,ORIGIN_ID_ORIGINAL) AS ORIGIN_PLANT_ID, --USED FOR LANE


--DESTINATION--
DEST.XD_FLAG AS DEST_XD_FLAG,
DEST.C2_ORG AS DEST_SOURCE_BUSINESS,
DEST.C2 AS DEST_PLANT_NAME,

  CASE 
    WHEN DEST.C2 LIKE 'PRO%' THEN
      SUBSTRING(DEST.C2, 4, POSITION('-' IN DEST.C2) - 4)
    ELSE
      SPLIT_PART(DEST.C2, '-', 1)
      END AS DESTINATION_ID_ORIGINAL,

IFNULL(CASE WHEN MODE_GROUP = 'LH' --USING LOGIC ABOVE FOR MODE_GROUP
    THEN DEST.LH_TRANSACTIONAL_ID
    ELSE DEST.STO_TRANSACTIONAL_ID
        END,
        DESTINATION_ID_ORIGINAL)
        AS DEST_PLANT_ID, --USED FOR LANE

CONCAT(ORIGIN_PLANT_ID,'-',DEST_PLANT_ID) AS SHIP_LANE


    
  FROM DM_DECISION_ANALYTICS.AD_HOC."2024_Carrier_Invoice" INV
    LEFT JOIN DM_DECISION_ANALYTICS.BENEFIT_TRACKING.DC_DIM_AL ORG
        ON ORG.C1 = INV.ORIGIN_NAME
        AND ORG.ZIP_5 = SPLIT_PART(INV.ORIGIN_ZIP, '-', 1)
    LEFT JOIN DM_DECISION_ANALYTICS.BENEFIT_TRACKING.DC_DIM_AL DEST
        ON DEST.C1 = INV.DEST_NAME
        AND DEST.ZIP_5 = SPLIT_PART(INV.DEST_ZIP, '-', 1)

  
  WHERE BU IN ('HDP','HDS')
),

activity_2025 AS (
  SELECT
    CREATE_DATE,
    NULL AS INVOICE_DATE,
    SHIP_DATE_2 AS ACTUAL_SHIP_2,
    SHIP_DATE AS ACTUAL_SHIP,
    BOL,
    PRO_NUMBER,
    CARRIER_SCAC AS INVOICE_SCAC,
    ORIGIN_NAME,
    ORIGIN_ZIP,
    ORIGIN_CODE,
    DEST_NAME,
    DEST_ZIP,
    DEST_CODE,
    CARRIER_CHARGE AS SETTLEMENT_TOTAL,
    NULL AS INVOICE_NUMBER,
    NULL AS INVOICE_GL,
    NULL AS EXTRACT_DATE,
    NULL AS EXTRACT_DATE_2,
    PO_NUMBER,
    NULL AS INVOICE_WEIGHT,
    NULL AS OWNER,
    NULL AS CARRIER_MODE,
    NULL AS GL_ACCOUNT_MATCH,
    NULL AS INVOICE_FISCAL_PERIOD,
    NULL AS ORIGIN_CC,
    NULL AS ORIGIN_DC_NAME,
    REPORTING_DC AS REPORTING_DC_NAME,
    REPORTING_CC,
    DIVISON_,
    REGION,
    NETWORK,
    BU,
    NULL AS EXTRACT_PERIOD,
    SHIP_PERIOD,
    SHIP_WEEK,
    SHIP_YEAR,
    -- LH_EXPENSE logic
    CASE
      WHEN INVOICE_MATCH = 'No Match'
           AND BOL NOT ILIKE '%Cancelled%'
           AND PO_NUMBER NOT ILIKE '%Overflow%'
           AND PO_NUMBER NOT ILIKE '%RTN%'
        THEN 'Contract'
      WHEN INVOICE_MATCH = 'No Match'
           AND BOL NOT ILIKE '%Cancelled%'
           AND PO_NUMBER ILIKE '%Overflow%'
        THEN 'Overflow'
      WHEN INVOICE_MATCH = 'No Match'
           AND BOL NOT ILIKE '%Cancelled%'
           AND PO_NUMBER ILIKE '%RTN%'
        THEN 'Return'
      ELSE NULL
    END AS LH_EXPENSE,
    -- _TRL flags
    CASE
      WHEN INVOICE_MATCH = 'No Match'
           AND BOL NOT ILIKE '%Cancelled%'
           AND PO_NUMBER NOT ILIKE '%Overflow%'
           AND PO_NUMBER NOT ILIKE '%RTN%' THEN 1 ELSE 0
    END AS CONTRACT_TRL,
    CASE
      WHEN INVOICE_MATCH = 'No Match'
           AND BOL NOT ILIKE '%Cancelled%'
           AND PO_NUMBER ILIKE '%Overflow%' THEN 1 ELSE 0
    END AS OVERFLOW_TRL,
    CASE
      WHEN INVOICE_MATCH = 'No Match'
           AND BOL NOT ILIKE '%Cancelled%'
           AND PO_NUMBER ILIKE '%RTN%' THEN 1 ELSE 0
    END AS RETURN_TRL,
    CASE
      WHEN INVOICE_MATCH = 'No Match'
           AND BOL NOT ILIKE '%Cancelled%'
           AND (
             PO_NUMBER ILIKE '%Overflow%' OR
             PO_NUMBER ILIKE '%RTN%' OR
             (PO_NUMBER NOT ILIKE '%Overflow%' AND PO_NUMBER NOT ILIKE '%RTN%')
           ) THEN 1 ELSE 0
    END AS LH_TRL,
    '2025_ACTIVITY' AS DATA_SOURCE,

    CASE 
        WHEN LH_EXPENSE IS NOT NULL
    THEN 'LH' 
    ELSE NULL
    END AS MODE_GROUP,

    --ORIGIN--
ORG.XD_FLAG AS ORIGIN_XD_FLAG,
ORG.C2 AS ORIGIN_PLANT_NAME, 

  CASE 
    WHEN ORG.C2 LIKE 'PRO%' THEN
      SUBSTRING(ORG.C2, 4, POSITION('-' IN ORG.C2) - 4)
    ELSE
      SPLIT_PART(ORG.C2, '-', 1)
      END AS ORIGIN_ID_ORIGINAL,

IFNULL(ORG.STO_TRANSACTIONAL_ID,ORIGIN_ID_ORIGINAL) AS ORIGIN_PLANT_ID, --USED FOR LANE


--DESTINATION--
DEST.XD_FLAG AS DEST_XD_FLAG,
DEST.C2_ORG AS DEST_SOURCE_BUSINESS,
DEST.C2 AS DEST_PLANT_NAME,

  CASE 
    WHEN DEST.C2 LIKE 'PRO%' THEN
      SUBSTRING(DEST.C2, 4, POSITION('-' IN DEST.C2) - 4)
    ELSE
      SPLIT_PART(DEST.C2, '-', 1)
      END AS DESTINATION_ID_ORIGINAL,

IFNULL(CASE WHEN MODE_GROUP = 'LH' --USING LOGIC ABOVE FOR MODE_GROUP
    THEN DEST.LH_TRANSACTIONAL_ID
    ELSE DEST.STO_TRANSACTIONAL_ID
        END,
        DESTINATION_ID_ORIGINAL)
        AS DEST_PLANT_ID, --USED FOR LANE

CONCAT(ORIGIN_PLANT_ID,'-',DEST_PLANT_ID) AS SHIP_LANE

  FROM DM_DECISION_ANALYTICS.AD_HOC."2025_Activity" INV
    LEFT JOIN DM_DECISION_ANALYTICS.BENEFIT_TRACKING.DC_DIM_AL ORG
        ON ORG.C1 = INV.ORIGIN_NAME
        AND ORG.ZIP_5 = SPLIT_PART(INV.ORIGIN_ZIP, '-', 1)
    LEFT JOIN DM_DECISION_ANALYTICS.BENEFIT_TRACKING.DC_DIM_AL DEST
        ON DEST.C1 = INV.DEST_NAME
        AND DEST.ZIP_5 = SPLIT_PART(INV.DEST_ZIP, '-', 1)

  
  WHERE BU IN ('HDP','HDS')
    
    )

#NAME?
SELECT * FROM carrier_2025
UNION ALL
SELECT * FROM carrier_2024
UNION ALL
SELECT * FROM activity_2025

    )
;


select 
*
from DM_DECISION_ANALYTICS.BENEFIT_TRACKING.LINEHAUL_ACTIVITY

// STEP 3 PEDRO
CREATE OR REPLACE TABLE DM_DECISION_ANALYTICS.BENEFIT_TRACKING.INTERFACILITY_DATA AS (

WITH SHIP_DATA AS (
SELECT DISTINCT
    LA.BOL,
    LA.SHIP_LANE,
    LA.SHIP_WEEK,
    LA.SHIP_YEAR,
    LA.MODE_GROUP,
    C.FISCAL_YR_ID,
    C.FISCAL_HALF,
    C.FISCAL_QUARTER,
    C.FISCAL_QTR_ID,
    C.FISCAL_PER_ID,
    C.FISCAL_PERIOD,
    C.FISCAL_WEEK,
    C.FISCAL_WEEK_ID,
    LA.LH_TRL AS TRL,
    LA.SETTLEMENT_TOTAL,

    
FROM DM_DECISION_ANALYTICS.BENEFIT_TRACKING.LINEHAUL_ACTIVITY LA
LEFT JOIN EDP.STR_MASTER_DATA.CALENDAR C
ON C.FISCAL_YR_ID = LA.SHIP_YEAR
AND C.FISCAL_WEEK = LA.SHIP_WEEK

WHERE MODE_GROUP = 'LH'

GROUP BY ALL

UNION

SELECT DISTINCT
    STO.BOL,
    STO.SHIP_LANE,
    STO.SHIP_WEEK,
    STO.SHIP_YEAR,
    STO.MODE_GROUP,
    C.FISCAL_YR_ID,
    C.FISCAL_HALF,
    C.FISCAL_QUARTER,
    C.FISCAL_QTR_ID,
    C.FISCAL_PER_ID,
    C.FISCAL_PERIOD,
    C.FISCAL_WEEK,
    C.FISCAL_WEEK_ID,
    STO.STO_TRL AS TRL,
    STO.SETTLEMENT_TOTAL


FROM DM_DECISION_ANALYTICS.BENEFIT_TRACKING.STO_ACTIVITY STO
LEFT JOIN EDP.STR_MASTER_DATA.CALENDAR C
ON C.FISCAL_YR_ID = STO.SHIP_YEAR
AND C.FISCAL_WEEK = STO.SHIP_WEEK

WHERE MODE_GROUP = 'STO'

GROUP BY ALL

)

,SHIP_INV AS (
SELECT DISTINCT
SI.SHIP_LANE,
SI.FISCAL_YR_ID,
SI.FISCAL_HALF,
SI.FISCAL_QUARTER,
SI.FISCAL_QTR_ID,
SI.FISCAL_PER_ID,
SI.FISCAL_PERIOD,
SI.FISCAL_WEEK,
SI.FISCAL_WEEK_ID,

SUM(CASE WHEN BOL NOT ILIKE '%CANCEL%' THEN TRL ELSE 0 END) AS TOTAL_TRAILERS,
SUM(CASE WHEN SI.MODE_GROUP = 'LH' AND BOL NOT ILIKE '%CANCEL%' THEN TRL ELSE 0 END) AS LH_TRAILERS,
SUM(CASE WHEN SI.MODE_GROUP = 'STO' AND BOL NOT ILIKE '%CANCEL%' THEN TRL ELSE 0 END) AS STO_TRAILERS,

SUM(CASE WHEN SI.MODE_GROUP IN ('LH','STO') THEN SI.SETTLEMENT_TOTAL END) AS TOTAL_SPEND,
SUM(CASE WHEN SI.MODE_GROUP = 'LH' THEN SI.SETTLEMENT_TOTAL END) AS LH_SPEND,
SUM(CASE WHEN SI.MODE_GROUP = 'STO' THEN SI.SETTLEMENT_TOTAL END) AS STO_SPEND

FROM SHIP_DATA SI

WHERE SI.MODE_GROUP IN ('LH','STO') 

GROUP BY ALL

    )
    
,LH AS (
SELECT DISTINCT
CONCAT(LHT.ORIGIN_PLANT_ID,'-',LHT.DEST_PLANT_ID) AS SHIP_LANE,
LHT.FISCAL_YR_ID,
LHT.FISCAL_HALF,
LHT.FISCAL_QUARTER,
LHT.FISCAL_QTR_ID,
LHT.FISCAL_PER_ID,
LHT.FISCAL_PERIOD,
LHT.FISCAL_WEEK,
LHT.FISCAL_WEEK_ID,
CAST(ROUND(SUM(LHT.LH_RPT_DLV_CBF), 4) AS NUMBER(18, 4)) AS LH_CUBE,
CAST(ROUND(SUM(LHT.DLV_COGS), 4) AS NUMBER(18, 4)) AS LH_COGS, 
CAST(ROUND(SUM(LHT.LH_RPT_DLV_WGT), 4) AS NUMBER(18, 4)) AS LH_WEIGHT

FROM DM_DECISION_ANALYTICS.BENEFIT_TRACKING.SAP_LH LHT

WHERE LHT.MODE_GROUP = 'LH'

GROUP BY ALL

    )
---------------------------
,STO AS (
SELECT DISTINCT
CONCAT(ST.SOURCE_DC,'-',ST.DEST_DC) AS SHIP_LANE,
ST.FISCAL_YR_ID,
ST.FISCAL_HALF,
ST.FISCAL_QUARTER,
ST.FISCAL_QTR_ID,
ST.FISCAL_PER_ID,
ST.FISCAL_PERIOD,
ST.FISCAL_WEEK,
ST.FISCAL_WEEK_ID,
CAST(ROUND(SUM(STO_RPT_SHIP_CBF), 4) AS NUMBER(18, 4)) AS STO_CUBE,
CAST(ROUND(SUM(SHIP_COGS), 4) AS NUMBER(18, 4)) AS STO_COGS,
CAST(ROUND(SUM(STO_RPT_SHIP_WGT), 4) AS NUMBER(18, 4)) AS STO_WEIGHT

FROM DM_DECISION_ANALYTICS.BENEFIT_TRACKING.STO_TRANSACTIONAL ST

WHERE ST.MODE_GROUP = 'STO'

GROUP BY ALL

    )
---------------------------
SELECT DISTINCT
SI.SHIP_LANE,
SI.FISCAL_YR_ID,
SI.FISCAL_HALF,
SI.FISCAL_QUARTER,
SI.FISCAL_QTR_ID,
SI.FISCAL_PER_ID,
SI.FISCAL_PERIOD,
SI.FISCAL_WEEK,
SI.FISCAL_WEEK_ID,

IFNULL(TOTAL_TRAILERS,0) AS TOTAL_TRAILERS,
IFNULL(LH_TRAILERS,0) AS LH_TRAILERS,
IFNULL(STO_TRAILERS,0) AS STO_TRAILERS,
IFNULL(TOTAL_SPEND,0) AS TOTAL_SPEND,
IFNULL(LH_SPEND,0) AS LH_SPEND,
IFNULL(STO_SPEND,0) AS STO_SPEND,


IFNULL(LH_CUBE,0)+IFNULL(STO_CUBE,0) AS TOTAL_CUBE,
IFNULL(LH_COGS,0)+IFNULL(STO_COGS,0) AS TOTAL_COGS,
IFNULL(LH_WEIGHT,0)+IFNULL(STO_WEIGHT,0) AS TOTAL_WEIGHT,

IFNULL(LH_CUBE,0) AS LH_CUBE,
IFNULL(LH_COGS,0) AS LH_COGS,
IFNULL(LH_WEIGHT,0) AS LH_WEIGHT,

IFNULL(STO_CUBE,0) AS STO_CUBE,
IFNULL(STO_COGS,0) AS STO_COGS,
IFNULL(STO_WEIGHT,0) AS STO_WEIGHT,

DLY_STO_PLN_GO_LIVE_DT,
DLY_STO_ACT_GO_LIVE_DT,
COLOAD_PLN_GO_LIVE_DT,
COLOAD_ACT_GO_LIVE_DT,
ROUTE_SORT_PLN_GO_LIVE_DT,
ROUTE_SORT_ACT_GO_LIVE_DT, 

CASE 
WHEN COLOAD_ACT_GO_LIVE_DT <= CURRENT_DATE THEN TRUE 
ELSE FALSE 
END AS GO_LIVE_FLAG

FROM SHIP_INV SI
LEFT JOIN LH
ON LH.SHIP_LANE = SI.SHIP_LANE
AND LH.FISCAL_YR_ID = SI.FISCAL_YR_ID
AND LH.FISCAL_HALF = SI.FISCAL_HALF
AND LH.FISCAL_QUARTER = SI.FISCAL_QUARTER
AND LH.FISCAL_QTR_ID = SI.FISCAL_QTR_ID
AND LH.FISCAL_PER_ID = SI.FISCAL_PER_ID
AND LH.FISCAL_PERIOD = SI.FISCAL_PERIOD
AND LH.FISCAL_WEEK = SI.FISCAL_WEEK
AND LH.FISCAL_WEEK_ID = SI.FISCAL_WEEK_ID

LEFT JOIN STO
ON STO.SHIP_LANE = SI.SHIP_LANE
AND STO.FISCAL_YR_ID = SI.FISCAL_YR_ID
AND STO.FISCAL_HALF = SI.FISCAL_HALF
AND STO.FISCAL_QUARTER = SI.FISCAL_QUARTER
AND STO.FISCAL_QTR_ID = SI.FISCAL_QTR_ID
AND STO.FISCAL_PER_ID = SI.FISCAL_PER_ID
AND STO.FISCAL_PERIOD = SI.FISCAL_PERIOD
AND STO.FISCAL_WEEK = SI.FISCAL_WEEK
AND STO.FISCAL_WEEK_ID = SI.FISCAL_WEEK_ID

LEFT JOIN DM_SUPPLYCHAIN.SCD_OPS_PROCESS.STO_LH_UTIL_IMPRV_SCHED S
ON CONCAT(S.SHIP_DC,'-',S.DEST_DC) = SI.SHIP_LANE

    )
;

CREATE OR REPLACE VIEW DM_DECISION_ANALYTICS.BENEFIT_TRACKING.INTRANETWORK_LANE_DATA_BY_PERIOD_RESULTS AS
WITH new_cost_data AS (
    -- New combined costs from LINEHAUL_ACTIVITY and STO_ACTIVITY with explicit type casting
    SELECT
        SHIP_YEAR AS fiscal_year,
        CASE
        WHEN SHIP_PERIOD = 'FEB' THEN 1
        WHEN SHIP_PERIOD = 'MAR' THEN 2
        WHEN SHIP_PERIOD = 'APR' THEN 3
        WHEN SHIP_PERIOD = 'MAY' THEN 4
        WHEN SHIP_PERIOD = 'JUN' THEN 5
        WHEN SHIP_PERIOD = 'JUL' THEN 6
        WHEN SHIP_PERIOD = 'AUG' THEN 7
        WHEN SHIP_PERIOD = 'SEP' THEN 8
        WHEN SHIP_PERIOD = 'OCT' THEN 9
        WHEN SHIP_PERIOD = 'NOV' THEN 10
        WHEN SHIP_PERIOD = 'DEC' THEN 11
        WHEN SHIP_PERIOD = 'JAN' THEN 12
        ELSE NULL
         END AS fiscal_period,
        SHIP_LANE,
        SUM(CAST(LH_COST AS FLOAT)) AS lh_spend,
        SUM(CAST(STO_COST AS FLOAT)) AS sto_spend,
        SUM(CAST(LH_COST AS FLOAT) + CAST(STO_COST AS FLOAT)) AS total_cost,
        SUM(truckload_lh_cost) AS truckload_lh_cost,
        SUM(truckload_sto_cost) AS truckload_sto_cost,
        SUM(truckload_sto_cost+truckload_lh_cost) AS total_truckload_cost,
        SUM(trailers) AS trailers,
        SUM(lh_cogs) AS lh_cogs,
        SUM(sto_cogs) AS sto_cogs
    FROM (
        SELECT
            SHIP_YEAR,
            SHIP_PERIOD,
            SHIP_WEEK,
            SHIP_LANE,
            CAST(SUM(SETTLEMENT_TOTAL) AS FLOAT) AS LH_COST,
            CAST(0 AS FLOAT) as STO_COST,
            SUM(lh_trl) as trailers,
            SUM(case when carrier_mode in ('Truckload', 'Intermodal') then SETTLEMENT_TOTAL else 0 end) as truckload_lh_cost,
            0 as truckload_sto_cost,
            0 AS lh_cogs,
            0 AS sto_cogs
        FROM DM_DECISION_ANALYTICS.BENEFIT_TRACKING.LINEHAUL_ACTIVITY
        WHERE LH_EXPENSE IS NOT NULL
        --AND SHIP_WEEK BETWEEN 1 and 13
        AND MODE_GROUP = 'LH'
        GROUP BY ALL
 
        UNION ALL
 
        SELECT
            SHIP_YEAR,
            SHIP_PERIOD,
            SHIP_WEEK,
            SHIP_LANE,
            CAST(0 AS FLOAT) AS LH_COST,
            CAST(SUM(SETTLEMENT_TOTAL) AS FLOAT) as STO_COST,
            SUM(sto_trl) as trailers,
            0 as truckload_lh_cost,
            SUM(case when carrier_mode in ('Truckload', 'Intermodal') then SETTLEMENT_TOTAL else 0 end) as truckload_sto_cost,
            0 AS lh_cogs,
            0 AS sto_cogs
        FROM DM_DECISION_ANALYTICS.BENEFIT_TRACKING.STO_ACTIVITY
        where 1=1
        AND MODE_GROUP = 'STO'
        GROUP BY ALL
    ) combined_costs
    WHERE
        SHIP_YEAR IN ('2024','2025')
       -- AND TRY_TO_NUMBER(SHIP_WEEK) BETWEEN 1 AND 13
    GROUP BY
        SHIP_YEAR,
        SHIP_PERIOD,
        SHIP_LANE
),
 
interfacility AS (
    SELECT
        ship_lane,
        fiscal_period,
        fiscal_yr_id,
        go_live_flag,
        CAST(SUM(TOTAL_COGS) AS FLOAT) AS ttl_cogs,
        CAST(SUM(LH_COGS) AS FLOAT) AS lh_cogs,
        CAST(SUM(STO_COGS) AS FLOAT) AS sto_cogs,
        CAST(SUM(TOTAL_CUBE) AS FLOAT) AS ttl_cube,
        CAST(SUM(TOTAL_WEIGHT) AS FLOAT) AS ttl_weight,
        CAST(SUM(TOTAL_TRAILERS) AS FLOAT) AS trailers
    FROM DM_DECISION_ANALYTICS.BENEFIT_TRACKING.INTERFACILITY_DATA
    GROUP BY ALL
),
 
shipments as (
    SELECT
        COALESCE(s.ship_lane, nc.SHIP_LANE) AS ship_lane,
        COALESCE(s.fiscal_period, nc.fiscal_period) AS fiscal_period,
        COALESCE(s.fiscal_yr_id, nc.fiscal_year) AS fiscal_year,
        /* Flag to identify data source */
        CASE
            WHEN s.ship_lane IS NULL THEN FALSE  -- Only in cost data
            WHEN nc.SHIP_LANE IS NULL THEN FALSE -- Only in interfacility data
            ELSE s.go_live_flag                  -- In both datasets
        END AS go_live_flag,
        /* Flag to identify data match status */
        CASE
            WHEN s.ship_lane IS NULL THEN 'Cost Only'
            WHEN nc.SHIP_LANE IS NULL THEN 'Volume Only'
            ELSE 'Complete Data'
        END AS data_status,
        CAST(SUM(COALESCE(s.ttl_cogs, 0)) AS FLOAT) AS ttl_cogs,
        CAST(SUM(COALESCE(s.lh_cogs, 0)) AS FLOAT) AS lh_cogs,
        CAST(SUM(COALESCE(s.sto_cogs, 0)) AS FLOAT) AS sto_cogs,
        CAST(SUM(COALESCE(s.ttl_cube, 0)) AS FLOAT) AS ttl_cube,
        CAST(SUM(COALESCE(s.ttl_weight, 0)) AS FLOAT) AS ttl_weight,
        CAST(SUM(COALESCE(nc.total_cost, 0)) AS FLOAT) AS cost,
        CAST(SUM(COALESCE(nc.lh_spend, 0)) AS FLOAT) AS lh_cost,
        CAST(SUM(COALESCE(nc.sto_spend, 0)) AS FLOAT) AS sto_cost,
        CAST(SUM(COALESCE(nc.truckload_lh_cost, 0)) AS FLOAT) AS truckload_lh_cost,
        CAST(SUM(COALESCE(nc.truckload_sto_cost, 0)) AS FLOAT) AS truckload_sto_cost,
        CAST(SUM(COALESCE(nc.total_truckload_cost, 0)) AS FLOAT) AS total_truckload_cost,
        /* Use interfacility trailers if available, otherwise use cost data trailers */
        CAST(SUM(COALESCE(s.trailers, COALESCE(nc.trailers, 0))) AS FLOAT) AS trailers,
        CAST(SUM(COALESCE(nc.lh_spend - nc.truckload_lh_cost, 0)) AS FLOAT) AS ltl_cost
    FROM interfacility s
    FULL OUTER JOIN new_cost_data nc
        ON s.ship_lane = nc.SHIP_LANE
        AND s.fiscal_yr_id = nc.fiscal_year
        AND s.fiscal_period = nc.fiscal_period
    WHERE COALESCE(s.fiscal_yr_id, nc.fiscal_year) IN ('2024','2025')
    GROUP BY ALL
),
 
year_2024 AS (
    SELECT
        ship_lane AS lane,
        fiscal_period,
        MAX(go_live_flag) AS go_live_flag,
        MAX(data_status) AS data_status_2024,
        CAST(SUM(ttl_cogs) AS FLOAT) AS cogs_2024,
        CAST(SUM(lh_cogs) AS FLOAT) AS lh_cogs_2024,
        CAST(SUM(sto_cogs) AS FLOAT) AS sto_cogs_2024,
        CAST(SUM(cost) AS FLOAT) AS cost_2024,
        CAST(SUM(lh_cost) AS FLOAT) AS lh_cost_2024,
        CAST(SUM(sto_cost) AS FLOAT) AS sto_cost_2024,
        CAST(SUM(truckload_lh_cost) AS FLOAT) AS truckload_lh_cost_2024,
        CAST(SUM(truckload_sto_cost) AS FLOAT) AS truckload_sto_cost_2024,
        CAST(SUM(total_truckload_cost) AS FLOAT) AS total_truckload_cost_2024,
        CAST(SUM(ltl_cost) AS FLOAT) AS ltl_cost_2024,
        CAST(SUM(trailers) AS FLOAT) AS trailers_2024,
        CAST(SUM(ttl_cube) AS FLOAT) AS cube_2024,
        CAST(SUM(ttl_weight) AS FLOAT) AS weight_2024,
       
        /* Calculate freight as % of COGS, safely handle division by zero */
        CASE WHEN SUM(ttl_cogs) = 0 THEN 0
             ELSE CAST(SUM(cost) AS FLOAT) / NULLIF(CAST(SUM(ttl_cogs) AS FLOAT), 0) END AS freight_pct_cogs_2024,
       
        /* Calculate other useful metrics */
        CASE WHEN SUM(trailers) = 0 THEN 0
             ELSE CAST(SUM(cost) AS FLOAT) / NULLIF(CAST(SUM(trailers) AS FLOAT), 0) END AS cost_per_trailer_2024,
             
        CASE WHEN SUM(trailers) = 0 THEN 0
             ELSE CAST(SUM(ttl_cube) AS FLOAT) / NULLIF(CAST(SUM(trailers) AS FLOAT), 0) END AS cube_per_trailer_2024
    FROM shipments
    WHERE fiscal_year = '2024'
    GROUP BY ship_lane, fiscal_period
),
 
year_2025 AS (
    SELECT
        ship_lane AS lane,
        fiscal_period,
        MAX(go_live_flag) AS go_live_flag,
        MAX(data_status) AS data_status_2025,
        CAST(SUM(ttl_cogs) AS FLOAT) AS cogs_2025,
        CAST(SUM(lh_cogs) AS FLOAT) AS lh_cogs_2025,
        CAST(SUM(sto_cogs) AS FLOAT) AS sto_cogs_2025,
        CAST(SUM(cost) AS FLOAT) AS cost_2025,
        CAST(SUM(lh_cost) AS FLOAT) AS lh_cost_2025,
        CAST(SUM(sto_cost) AS FLOAT) AS sto_cost_2025,
        CAST(SUM(truckload_lh_cost) AS FLOAT) AS truckload_lh_cost_2025,
        CAST(SUM(truckload_sto_cost) AS FLOAT) AS truckload_sto_cost_2025,
        CAST(SUM(total_truckload_cost) AS FLOAT) AS total_truckload_cost_2025,
        CAST(SUM(ltl_cost) AS FLOAT) AS ltl_cost_2025,
        CAST(SUM(trailers) AS FLOAT) AS trailers_2025,
        CAST(SUM(ttl_cube) AS FLOAT) AS cube_2025,
        CAST(SUM(ttl_weight) AS FLOAT) AS weight_2025,
       
        /* Calculate freight as % of COGS */
        CASE WHEN SUM(ttl_cogs) = 0 THEN 0
             ELSE CAST(SUM(cost) AS FLOAT) / NULLIF(CAST(SUM(ttl_cogs) AS FLOAT), 0) END AS freight_pct_cogs_2025,
             
        /* Calculate other useful metrics */
        CASE WHEN SUM(trailers) = 0 THEN 0
             ELSE CAST(SUM(cost) AS FLOAT) / NULLIF(CAST(SUM(trailers) AS FLOAT), 0) END AS cost_per_trailer_2025,
             
        CASE WHEN SUM(trailers) = 0 THEN 0
             ELSE CAST(SUM(ttl_cube) AS FLOAT) / NULLIF(CAST(SUM(trailers) AS FLOAT), 0) END AS cube_per_trailer_2025
    FROM shipments
    WHERE fiscal_year = '2025'
    GROUP BY ship_lane, fiscal_period
),
 
joined AS (
    SELECT
        COALESCE(y0.lane, y1.lane) AS lane,
        COALESCE(y0.fiscal_period, y1.fiscal_period) AS fiscal_period,
        COALESCE(y1.go_live_flag, y0.go_live_flag) AS go_live_flag,
        /* Tracking data quality */
        COALESCE(y0.data_status_2024, 'Missing 2024') AS data_status_2024,
        COALESCE(y1.data_status_2025, 'Missing 2025') AS data_status_2025,
        CASE
            WHEN y0.data_status_2024 = 'Complete Data' AND y1.data_status_2025 = 'Complete Data' THEN 'Complete'
            WHEN y0.lane IS NULL THEN '2025 Only'
            WHEN y1.lane IS NULL THEN '2024 Only'
            ELSE 'Partial Data'
        END AS data_completeness,
       
        /* 2024 Data */
        COALESCE(y0.cogs_2024, 0) AS cogs_2024,
        COALESCE(y0.lh_cogs_2024, 0) AS lh_cogs_2024,
        COALESCE(y0.sto_cogs_2024, 0) AS sto_cogs_2024,
        COALESCE(y0.cost_2024, 0) AS cost_2024,
        COALESCE(y0.lh_cost_2024, 0) AS lh_cost_2024,
        COALESCE(y0.sto_cost_2024, 0) AS sto_cost_2024,
        COALESCE(y0.truckload_lh_cost_2024, 0) AS truckload_lh_cost_2024,
        COALESCE(y0.truckload_sto_cost_2024, 0) AS truckload_sto_cost_2024,
        COALESCE(y0.total_truckload_cost_2024, 0) AS total_truckload_cost_2024,
        COALESCE(y0.ltl_cost_2024, 0) AS ltl_cost_2024,
        COALESCE(y0.trailers_2024, 0) AS trailers_2024,
        COALESCE(y0.cube_2024, 0) AS cube_2024,
        COALESCE(y0.weight_2024, 0) AS weight_2024,
        COALESCE(y0.freight_pct_cogs_2024, 0) AS freight_pct_cogs_2024,
        COALESCE(y0.cost_per_trailer_2024, 0) AS cost_per_trailer_2024,
        COALESCE(y0.cube_per_trailer_2024, 0) AS cube_per_trailer_2024,
       
        /* 2025 Data */
        COALESCE(y1.cogs_2025, 0) AS cogs_2025,
        COALESCE(y1.lh_cogs_2025, 0) AS lh_cogs_2025,
        COALESCE(y1.sto_cogs_2025, 0) AS sto_cogs_2025,
        COALESCE(y1.cost_2025, 0) AS cost_2025,
        COALESCE(y1.lh_cost_2025, 0) AS lh_cost_2025,
        COALESCE(y1.sto_cost_2025, 0) AS sto_cost_2025,
        COALESCE(y1.truckload_lh_cost_2025, 0) AS truckload_lh_cost_2025,
        COALESCE(y1.truckload_sto_cost_2025, 0) AS truckload_sto_cost_2025,
        COALESCE(y1.total_truckload_cost_2025, 0) AS total_truckload_cost_2025,
        COALESCE(y1.ltl_cost_2025, 0) AS ltl_cost_2025,
        COALESCE(y1.trailers_2025, 0) AS trailers_2025,
        COALESCE(y1.cube_2025, 0) AS cube_2025,
        COALESCE(y1.weight_2025, 0) AS weight_2025,
        COALESCE(y1.freight_pct_cogs_2025, 0) AS freight_pct_cogs_2025,
        COALESCE(y1.cost_per_trailer_2025, 0) AS cost_per_trailer_2025,
        COALESCE(y1.cube_per_trailer_2025, 0) AS cube_per_trailer_2025
    FROM year_2024 y0
    FULL OUTER JOIN year_2025 y1
        ON y0.lane = y1.lane
        AND y0.fiscal_period = y1.fiscal_period
),
 
waterfall AS (
    SELECT
        j.lane,
        j.fiscal_period,
        j.go_live_flag,
        j.data_status_2024,
        j.data_status_2025,
        j.data_completeness,
        j.cogs_2024,
        j.lh_cogs_2024,
        j.sto_cogs_2024,
        j.cost_2024,
        j.lh_cost_2024,
        j.sto_cost_2024,
        j.truckload_lh_cost_2024,
        j.truckload_sto_cost_2024,
        j.total_truckload_cost_2024,
        j.ltl_cost_2024,
        j.freight_pct_cogs_2024,
        j.cogs_2025,
        j.lh_cogs_2025,
        j.sto_cogs_2025,
        j.cost_2025,
        j.lh_cost_2025,
        j.sto_cost_2025,
        j.truckload_lh_cost_2025,
        j.truckload_sto_cost_2025,
        j.total_truckload_cost_2025,
        j.ltl_cost_2025,
        j.freight_pct_cogs_2025,
        j.trailers_2024,
        j.trailers_2025,
        j.cube_2024,
        j.cube_2025,
        j.weight_2024,
        j.weight_2025,
        j.cube_per_trailer_2024,
        j.cube_per_trailer_2025,
        j.cost_per_trailer_2024,
        j.cost_per_trailer_2025,
       
        /* Actual difference */
        CAST((j.cost_2025 - j.cost_2024) AS FLOAT) AS total_cost_diff,
       
        /* Define minimum thresholds for reliable analysis */
        CASE
            WHEN j.cogs_2024 < 10000 OR j.cogs_2025 < 10000 THEN 'Low'
            WHEN j.cogs_2024 < 50000 OR j.cogs_2025 < 50000 THEN 'Medium'
            ELSE 'High'
        END AS volume_confidence,
       
        /* Cap freight percentage at reasonable levels */
        LEAST(j.freight_pct_cogs_2024, 0.50) AS capped_freight_pct_2024,
        LEAST(j.freight_pct_cogs_2025, 0.50) AS capped_freight_pct_cogs_2025,
       
        /* Effect 1: COGS Volume Effect - if COGS changed but rate stayed the same */
        CASE
            WHEN j.cogs_2024 < 10000 OR j.cogs_2025 < 10000 THEN
                CAST((j.cogs_2025 - j.cogs_2024) * LEAST(j.freight_pct_cogs_2024, 0.50) AS FLOAT)
            ELSE
                CAST((j.cogs_2025 - j.cogs_2024) * j.freight_pct_cogs_2024 AS FLOAT)
        END AS cogs_volume_effect,
       
        /* Effect 2: FCOGS Leverage - change in freight as % of COGS (renamed from fcogs_leverage) */
        CASE
            WHEN j.cogs_2024 < 10000 OR j.cogs_2025 < 10000 THEN
                CAST(j.cogs_2025 * (LEAST(j.freight_pct_cogs_2025, 0.50) - LEAST(j.freight_pct_cogs_2024, 0.50)) AS FLOAT)
            ELSE
                CAST(j.cogs_2025 * (j.freight_pct_cogs_2025 - j.freight_pct_cogs_2024) AS FLOAT)
        END AS fcogs_leverage,
       
        /* Effect 3: Cost per trailer rate impact */
        CASE
            WHEN j.trailers_2025 = 0 THEN 0
            ELSE CAST((j.cost_per_trailer_2025 - j.cost_per_trailer_2024) * j.trailers_2025 AS FLOAT)
        END AS cost_per_trailer_rate_impact,
       
        /* Raw values (uncapped) for reference */
        CAST((j.cogs_2025 * j.freight_pct_cogs_2024 - j.cost_2024) AS FLOAT) AS raw_cogs_volume_effect,
        CAST((j.cogs_2025 * j.freight_pct_cogs_2025 - j.cogs_2025 * j.freight_pct_cogs_2024) AS FLOAT) AS raw_fcogs_leverage,
       
        /* % change in metrics */
        CASE
            WHEN j.cogs_2024 = 0 THEN NULL
            ELSE CAST((j.cogs_2025 - j.cogs_2024) / NULLIF(j.cogs_2024, 0) AS FLOAT)
        END AS cogs_pct_change,
       
        CASE
            WHEN j.lh_cogs_2024 = 0 THEN NULL
            ELSE CAST((j.lh_cogs_2025 - j.lh_cogs_2024) / NULLIF(j.lh_cogs_2024, 0) AS FLOAT)
        END AS lh_cogs_pct_change,
       
        CASE
            WHEN j.sto_cogs_2024 = 0 THEN NULL
            ELSE CAST((j.sto_cogs_2025 - j.sto_cogs_2024) / NULLIF(j.sto_cogs_2024, 0) AS FLOAT)
        END AS sto_cogs_pct_change,
       
        CASE
            WHEN j.cost_2024 = 0 THEN NULL
            ELSE CAST((j.cost_2025 - j.cost_2024) / NULLIF(j.cost_2024, 0) AS FLOAT)
        END AS cost_pct_change,
       
        CASE
            WHEN j.lh_cost_2024 = 0 THEN NULL
            ELSE CAST((j.lh_cost_2025 - j.lh_cost_2024) / NULLIF(j.lh_cost_2024, 0) AS FLOAT)
        END AS lh_cost_pct_change,
       
        CASE
            WHEN j.sto_cost_2024 = 0 THEN NULL
            ELSE CAST((j.sto_cost_2025 - j.sto_cost_2024) / NULLIF(j.sto_cost_2024, 0) AS FLOAT)
        END AS sto_cost_pct_change,
       
        CASE
            WHEN j.trailers_2024 = 0 THEN NULL
            ELSE CAST((j.trailers_2025 - j.trailers_2024) / NULLIF(j.trailers_2024, 0) AS FLOAT)
        END AS trailers_pct_change,
       
        CASE
            WHEN j.cube_2024 = 0 THEN NULL
            ELSE CAST((j.cube_2025 - j.cube_2024) / NULLIF(j.cube_2024, 0) AS FLOAT)
        END AS cube_pct_change,
       
        CASE
            WHEN j.weight_2024 = 0 THEN NULL
            ELSE CAST((j.weight_2025 - j.weight_2024) / NULLIF(j.weight_2024, 0) AS FLOAT)
        END AS weight_pct_change,
       
        CASE
            WHEN j.cube_per_trailer_2024 = 0 THEN NULL
            ELSE CAST((j.cube_per_trailer_2025 - j.cube_per_trailer_2024) / NULLIF(j.cube_per_trailer_2024, 0) AS FLOAT)
        END AS cube_per_trailer_pct_change,
 
        fcogs_leverage - cost_per_trailer_rate_impact AS MVL_UTIL_IMPACT
       
        /* Add MVL_UTIL_IMPACT to ensure complete waterfall (renamed from residual_effect) */
        -- CASE
        --     WHEN j.cogs_2024 < 10000 OR j.cogs_2025 < 10000 THEN
        --         CAST(j.cost_2025 - j.cost_2024 -
        --             ((j.cogs_2025 - j.cogs_2024) * LEAST(j.freight_pct_cogs_2024, 0.50)) -
        --             (j.cogs_2025 * (LEAST(j.freight_pct_cogs_2025, 0.50) - LEAST(j.freight_pct_cogs_2024, 0.50))) -
        --             ((j.cost_per_trailer_2025 - j.cost_per_trailer_2024) * j.trailers_2025) AS FLOAT)
        --     ELSE
        --         CAST(j.cost_2025 - j.cost_2024 -
        --             ((j.cogs_2025 - j.cogs_2024) * j.freight_pct_cogs_2024) -
        --             (j.cogs_2025 * (j.freight_pct_cogs_2025 - j.freight_pct_cogs_2024)) -
        --             ((j.cost_per_trailer_2025 - j.cost_per_trailer_2024) * j.trailers_2025) AS FLOAT)
        -- END AS MVL_UTIL_IMPACT
    FROM joined j
)
 
SELECT
    lane,
    fiscal_period,
    go_live_flag,
    /* Data quality flags */
    data_status_2024,
    data_status_2025,
    data_completeness,
    volume_confidence,
   
    /* COGS and Cost metrics */
    cogs_2024,
    cogs_2025,
    cogs_pct_change,
    lh_cogs_2024,
    lh_cogs_2025,
    lh_cogs_pct_change,
    sto_cogs_2024,
    sto_cogs_2025,
    sto_cogs_pct_change,
    cost_2024,
    cost_2025,
    total_cost_diff,
    cost_pct_change,
   
    /* Detailed cost breakdowns - 2024 */
    lh_cost_2024,
    sto_cost_2024,
    truckload_lh_cost_2024,
    truckload_sto_cost_2024,
    total_truckload_cost_2024,
    ltl_cost_2024,
   
    /* Detailed cost breakdowns - 2025 */
    lh_cost_2025,
    sto_cost_2025,
    truckload_lh_cost_2025,
    truckload_sto_cost_2025,
    total_truckload_cost_2025,
    ltl_cost_2025,
   
    /* Cost percentage changes */
    lh_cost_pct_change,
    sto_cost_pct_change,
   
    /* Volume metrics */
    cube_2024,
    cube_2025,
    cube_pct_change,
    weight_2024,
    weight_2025,
    weight_pct_change,
   
    /* Rate and volume effects - simplified waterfall */
    freight_pct_cogs_2024,
    freight_pct_cogs_2025,
    cogs_volume_effect,
    fcogs_leverage,
    cost_per_trailer_rate_impact,
    MVL_UTIL_IMPACT,
   
    /* Waterfall validation - should be close to zero */
    total_cost_diff - cogs_volume_effect - fcogs_leverage - cost_per_trailer_rate_impact - MVL_UTIL_IMPACT AS waterfall_check,
   
    /* Trailers and efficiency metrics */
    trailers_2024,
    trailers_2025,
    trailers_pct_change,
    cost_per_trailer_2024,
    cost_per_trailer_2025,
    cube_per_trailer_2024,
    cube_per_trailer_2025,
    cube_per_trailer_pct_change,
   
    /* Interpret the savings */
    CASE
        WHEN total_cost_diff < 0 THEN 'Savings'
        WHEN total_cost_diff > 0 THEN 'Cost Increase'
        ELSE 'No Change'
    END AS cost_trend,
   
    CASE
        WHEN freight_pct_cogs_2025 < freight_pct_cogs_2024 THEN 'Improved Efficiency'
        WHEN freight_pct_cogs_2025 > freight_pct_cogs_2024 THEN 'Decreased Efficiency'
        ELSE 'No Change in Efficiency'
    END AS efficiency_trend
FROM waterfall
ORDER BY
    fiscal_period,
    go_live_flag DESC,
    volume_confidence DESC,
    ABS(total_cost_diff) DESC;