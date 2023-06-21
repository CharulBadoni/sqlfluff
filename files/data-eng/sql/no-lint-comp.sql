CREATE TEMPORARY FUNCTION convert_values(message STRING)
RETURNS STRING
  LANGUAGE js AS """
  if (message == 'false'){
  return 0
  }
  else if(message == 'true'){
    return 1
  }
  var value = Number(message)
  return value

""";


SELECT '' AS blocked_advertising_app_id, '' AS blocked_advertising_app_name,
           t.blocked_company_id AS blocked_advertising_company_id, t.blocked_company_name AS blocked_advertising_company_name,
           t.blocking_company_id AS blocking_publishing_company_id, t.blocking_company_name AS blocking_publishing_company_name,
           t.blocking_app_id AS blocking_publisher_app_id, t.blocking_app_name AS blocking_publisher_app_name,
           t.block_type,
           COALESCE(u1.impressions, 0) AS blocking_publishing_impressions, COALESCE(u1.money_spent, 0) AS blocking_publishing_money_spent, COALESCE(u1.money_earned, 0) AS blocking_publishing_money_earned,
           u2.app_platform_name, u2.region_name,
           COALESCE(u2.impressions, 0) AS blocked_advertising_impressions, COALESCE(u2.money_spent, 0) AS blocked_advertising_money_spent, COALESCE(u2.money_earned, 0) AS blocked_advertising_money_earned,
           sf.supply_management_region, sf.prm, sf.bd,
           DATE_SUB("{{ data_interval_start.in_timezone('US/Pacific').strftime('%Y-%m-%d') }}", interval 1 day ) AS update_dt
      FROM
            (
            SELECT DISTINCT
                   -- b.blocking_campaign_id, c.campaign_name AS blocking_campaign_name,
                   co.company_id AS blocking_company_id, co.company_name AS blocking_company_name,
                   a.app_id AS blocking_app_id, a.app_name AS blocking_app_name,
                   'company' AS block_type,
                   b.blocked_company_id, co2.company_name AS blocked_company_name
              FROM
                        (
                        -- Get company level block, i.e. Campaign ID blocks Company ID
                        with data as (
                      SELECT REPLACE(JSON_EXTRACT(json, '$._id.mongo_id_oid'),'"','') AS blocking_campaign_id,
                        JSON_EXTRACT_ARRAY(json, '$.ignored_company_ids') AS blocked_company_id1
                        FROM `bi.campaigns_json`
                        WHERE dt = DATE_SUB("{{ data_interval_start.in_timezone('US/Pacific').strftime('%Y-%m-%d') }}", interval 1 day)
                        and COALESCE(CAST(convert_values(JSON_EXTRACT(json, '$.switches.campaign_switch')) as INT64), 1) = 1
                        )
                        select Distinct
                        blocking_campaign_id,
                        REPLACE(JSON_EXTRACT(blocked_company_id2, '$.mongo_id_oid'),'"','') AS blocked_company_id
                         from data, UNNEST (blocked_company_id1) as blocked_company_id2
                        ) b
                  JOIN
                  (
                      -- Join with publishing campaigns to get blocking Company ID and Campaign Name.
             SELECT campaign_id, MAX(app_id) AS app_id, MAX(company_id) AS company_id, MAX(campaign_name) AS campaign_name
               FROM `dimensions.campaign`
              WHERE is_publishing_app = 1 -- blocking company is a publisher.
              GROUP BY campaign_id
            ) c ON b.blocking_campaign_id = c.campaign_id
             -- Get blocking Company Name
                  JOIN `dimensions.company` co ON c.company_id = co.company_id
          -- Get blocking App Name
          JOIN `dimensions.app` a ON c.app_id = a.app_id
                  -- Get blocked Company Name
                  JOIN `dimensions.company` co2 ON b.blocked_company_id = co2.company_id
            ) t
          -- number of impressions served by publisher.
          JOIN
          (
            -- Metrics for blocking publisher.
            SELECT co.company_id, co.company_name,
                 c2.app_id,
                   SUM(d.impressions) AS impressions, SUM(d.money_spent) AS money_spent, SUM(d.money_earned) AS money_earned
           FROM `warehouse.daily_uber_aggr` d
                JOIN
                -- network data only
                (SELECT DISTINCT campaign_id FROM `dimensions.campaign` WHERE campaign_type = 3) ct ON d.advertiser_campaign = ct.campaign_id
                JOIN
                -- Get company_id and app_id
                (SELECT campaign_id, MAX(app_id) AS app_id, MAX(company_id) AS company_id FROM `dimensions.campaign` WHERE is_publishing_app = 1 GROUP BY campaign_id) c2 ON d.publisher_campaign = c2.campaign_id
                JOIN `dimensions.company` co ON c2.company_id = co.company_id
             WHERE d.dt >= DATE_SUB("{{ data_interval_start.in_timezone('US/Pacific').strftime('%Y-%m-%d') }}", interval 30 day )
            AND d.dt <= DATE_SUB("{{ data_interval_start.in_timezone('US/Pacific').strftime('%Y-%m-%d') }}", interval 1 day )
             GROUP BY co.company_id, co.company_name, c2.app_id
             HAVING SUM(d.impressions) > 0 OR SUM(d.money_spent) > 0 OR SUM(d.money_earned) > 0
          ) u1 ON t.blocking_company_id = u1.company_id AND t.blocking_app_id = u1.app_id
          JOIN
          (
            -- Metrics for blocked advertiser.
            SELECT ct.company_id AS advertising_company_id, app_platform_name, COALESCE(r.region_name, 'Rest of World') AS region_name, SUM(d.impressions) AS impressions, SUM(d.money_spent) AS money_spent, SUM(d.money_earned) AS money_earned
           FROM `warehouse.daily_uber_aggr` d
                JOIN
                -- network data only
                (SELECT campaign_id, MAX(company_id) AS company_id, MAX(app_platform_name) AS app_platform_name FROM `dimensions.campaign` WHERE campaign_type = 3 AND is_advertising_app = 1 GROUP BY campaign_id) ct ON d.advertiser_campaign = ct.campaign_id
                LEFT OUTER JOIN `dimensions.regions` r ON d.iso_country_code = r.country_code
             WHERE d.dt >= DATE_SUB("{{ data_interval_start.in_timezone('US/Pacific').strftime('%Y-%m-%d') }}", interval 30 day)
            AND d.dt < "{{ data_interval_start.in_timezone('US/Pacific').strftime('%Y-%m-%d') }}"
            GROUP BY ct.company_id, app_platform_name, region_name
         HAVING SUM(d.impressions) > 0 OR SUM(d.money_spent) > 0 OR SUM(d.money_earned) > 0
          ) u2 ON t.blocked_company_id = u2.advertising_company_id
          -- Get account metadata for blocked company.
          LEFT OUTER JOIN (SELECT * FROM `bi.sfdc_accounts_users` WHERE dt = DATE_SUB("{{ data_interval_start.in_timezone('US/Pacific').strftime('%Y-%m-%d') }}", interval 1 day) )sf ON t.blocking_company_id = sf.company_id
