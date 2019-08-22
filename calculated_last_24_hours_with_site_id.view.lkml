view: calculated_last_24_hours_with_site_id {
  derived_table: {
    sql: WITH calculated_view AS (WITH crosstab_view AS (select *
      from crosstab (
          '
      select filename,property_id,max(value)
              from metric_events
          where
            filename in (
            select distinct filename
            from metric_events
            where
              (
              property_id = (
                select id from metric_property
                where
                  category = ''event_import'' and
                  module_name = ''ftp-import-monitor'' and
                  property_name = ''end_time''
                limit 1
              )
              ) and
              cast(value as timestamp) >= CURRENT_TIMESTAMP - interval '{% parameter metric_selector %}'
              and cast(value as timestamp) < CURRENT_TIMESTAMP
            ) and
            property_id in
            (
            select distinct id
            from metric_property
            where
              property_type = ''varchar''
            )
          group by 1,2
      UNION
      select filename, property_id, cast(sum(cast(value as integer)) as varchar) as value from metric_events
          where
            filename in (
            select distinct filename
            from metric_events
            where
              (
              property_id = (
                select id from metric_property
                where
                  category = ''event_import'' and
                  module_name = ''ftp-import-monitor'' and
                  property_name = ''end_time''
                limit 1
              )
              ) and
              cast(value as timestamp) >= CURRENT_TIMESTAMP - interval '{% parameter metric_selector %}'
              and cast(value as timestamp) < CURRENT_TIMESTAMP
            ) and
            property_id in
            (
            select distinct id
            from metric_property
            where
              property_type = ''integer''
            )
          group by 1,2
      order by 1,2

        ',
        'select id from metric_property order by 1 ')
          AS (filename varchar, start_time varchar, end_time varchar, total varchar,
            success varchar, failure varchar,
            sf_last_seeen varchar, sf_count varchar,
            athena_last_seen varchar, athena_count varchar, drop_time varchar)
       )
SELECT
  crosstab_view.filename  AS "crosstab_view.filename",
  crosstab_view.total  AS "crosstab_view.total",
  crosstab_view.drop_time  AS "crosstab_view.drop_time",
  crosstab_view.success  AS "crosstab_view.success",
  crosstab_view.failure  AS "crosstab_view.failure",
  cast(crosstab_view.failure as float) / cast(crosstab_view.total as float) * 100  AS "crosstab_view.error_rate",
  crosstab_view.end_time  AS "crosstab_view.end_time",
  date_part('epoch', cast(crosstab_view.end_time as timestamp)) - date_part('epoch', cast(crosstab_view.start_time as timestamp))  AS "crosstab_view.duration",
  cast(crosstab_view.success as float) / (date_part('epoch', cast(crosstab_view.end_time as timestamp)) - date_part('epoch', cast(crosstab_view.start_time as timestamp)))  AS "crosstab_view.ingestion-rate",
  crosstab_view.sf_count  AS "crosstab_view.sf_count",
  crosstab_view.sf_last_seeen  AS "crosstab_view.sf_last_seeen",
  crosstab_view.athena_count  AS "crosstab_view.athena_count",
  crosstab_view.athena_last_seen  AS "crosstab_view.athena_last_seen",
  metric_events.site_id as "site_id"
FROM crosstab_view
join metric_events on crosstab_view.filename=metric_events.filename
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
ORDER BY 1
 )
select
  calculated_view."site_id",
  calculated_view."crosstab_view.filename"  AS "calculated_view.crosstab_view_filename",
  calculated_view."crosstab_view.total"  AS "calculated_view.crosstab_view_total",
  calculated_view."crosstab_view.drop_time"  AS "calculated_view.crosstab_view_drop_time",
  calculated_view."crosstab_view.success"  AS "calculated_view.crosstab_view_success",
  calculated_view."crosstab_view.failure"  AS "calculated_view.crosstab_view_failure",
  calculated_view."crosstab_view.error_rate"  AS "calculated_view.crosstab_view_error_rate",
  calculated_view."crosstab_view.end_time"  AS "calculated_view.crosstab_view_end_time",
  calculated_view."crosstab_view.duration"  AS "calculated_view.crosstab_view_duration",
  calculated_view."crosstab_view.ingestion-rate"  AS "calculated_view.crosstab_view_ingestionrate",
  calculated_view."crosstab_view.sf_count"  AS "calculated_view.crosstab_view_sf_count",
  calculated_view."crosstab_view.sf_last_seeen"  AS "calculated_view.crosstab_view_sf_last_seeen",
  calculated_view."crosstab_view.athena_count"  AS "calculated_view.crosstab_view_athena_count",
  calculated_view."crosstab_view.athena_last_seen"  AS "calculated_view.crosstab_view_athena_last_seen"
FROM calculated_view
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
ORDER BY 1
 ;;
  }

  parameter: metric_selector {
    type: string
    label: "Predefined filter"
    allowed_value: {
      label: "5 minutes"
      value: "5 minutes"
    }
    allowed_value: {
      label: "15 minutes"
      value: "15 minutes"
    }
    allowed_value: {
      label: "30 minutes"
      value: "30 minutes"
    }
    allowed_value: {
      label: "1 hour"
      value: "1 H"
    }
    allowed_value: {
      label: "4 hours"
      value: "4 H"
    }
    allowed_value: {
      label: "8 hours"
      value: "8 H"
    }
    allowed_value: {
      label: "12 hours"
      value: "12 H"
    }
    allowed_value: {
      label: "1 day"
      value: "24 H"
    }
    allowed_value: {
      label: "3 days"
      value: "72 H"
    }
    allowed_value: {
      label: "5 days"
      value: "120 H"
    }
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: site_id {
    type: string
    sql: ${TABLE}.site_id ;;
  }

  dimension: calculated_view_crosstab_view_filename {
    type: string
    sql: ${TABLE}."calculated_view.crosstab_view_filename" ;;
  }

  dimension: calculated_view_crosstab_view_total {
    type: string
    sql: ${TABLE}."calculated_view.crosstab_view_total" ;;
  }

  dimension: calculated_view_crosstab_view_drop_time {
    type: string
    sql: ${TABLE}."calculated_view.crosstab_view_drop_time" ;;
  }

  dimension: calculated_view_crosstab_view_success {
    type: string
    sql: ${TABLE}."calculated_view.crosstab_view_success" ;;
  }

  dimension: calculated_view_crosstab_view_failure {
    type: string
    sql: ${TABLE}."calculated_view.crosstab_view_failure" ;;
  }

  dimension: calculated_view_crosstab_view_error_rate {
    type: number
    sql: ${TABLE}."calculated_view.crosstab_view_error_rate" ;;
  }

  dimension: calculated_view_crosstab_view_end_time {
    type: string
    sql: ${TABLE}."calculated_view.crosstab_view_end_time" ;;
  }

  dimension: calculated_view_crosstab_view_duration {
    type: number
    sql: ${TABLE}."calculated_view.crosstab_view_duration" ;;
  }

  dimension: calculated_view_crosstab_view_ingestionrate {
    type: number
    sql: ${TABLE}."calculated_view.crosstab_view_ingestionrate" ;;
  }

  dimension: calculated_view_crosstab_view_sf_count {
    type: string
    sql: ${TABLE}."calculated_view.crosstab_view_sf_count" ;;
  }

  dimension: calculated_view_crosstab_view_sf_last_seeen {
    type: string
    sql: ${TABLE}."calculated_view.crosstab_view_sf_last_seeen" ;;
  }

  dimension: calculated_view_crosstab_view_athena_count {
    type: string
    sql: ${TABLE}."calculated_view.crosstab_view_athena_count" ;;
  }

  dimension: calculated_view_crosstab_view_athena_last_seen {
    type: string
    sql: ${TABLE}."calculated_view.crosstab_view_athena_last_seen" ;;
  }

  set: detail {
    fields: [
      site_id,
      calculated_view_crosstab_view_filename,
      calculated_view_crosstab_view_total,
      calculated_view_crosstab_view_drop_time,
      calculated_view_crosstab_view_success,
      calculated_view_crosstab_view_failure,
      calculated_view_crosstab_view_error_rate,
      calculated_view_crosstab_view_end_time,
      calculated_view_crosstab_view_duration,
      calculated_view_crosstab_view_ingestionrate,
      calculated_view_crosstab_view_sf_count,
      calculated_view_crosstab_view_sf_last_seeen,
      calculated_view_crosstab_view_athena_count,
      calculated_view_crosstab_view_athena_last_seen
    ]
  }
}
