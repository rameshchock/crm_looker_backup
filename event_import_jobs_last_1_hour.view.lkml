view: event_import_jobs_last_1_hour {
  derived_table: {
    sql: select *
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
        property_id = 1 or
        property_id = 2
        ) and
        cast(value as timestamp) >= CURRENT_TIMESTAMP - interval ''1 H''
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
        property_id = 1 or
        property_id = 2
        ) and
        cast(value as timestamp) >= CURRENT_TIMESTAMP - interval ''1 H''
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
      athena_last_seen varchar, athena_count varchar)
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: filename {
    type: string
    sql: ${TABLE}.filename ;;
  }

  dimension: start_time {
    type: string
    sql: ${TABLE}.start_time ;;
  }

  dimension: end_time {
    type: string
    sql: ${TABLE}.end_time ;;
  }

  dimension: total {
    type: string
    sql: ${TABLE}.total ;;
  }

  dimension: success {
    type: string
    sql: ${TABLE}.success ;;
  }

  dimension: failure {
    type: string
    sql: ${TABLE}.failure ;;
  }

  dimension: sf_last_seeen {
    type: string
    sql: ${TABLE}.sf_last_seeen ;;
  }

  dimension: sf_count {
    type: string
    sql: ${TABLE}.sf_count ;;
  }

  dimension: athena_last_seen {
    type: string
    sql: ${TABLE}.athena_last_seen ;;
  }

  dimension: athena_count {
    type: string
    sql: ${TABLE}.athena_count ;;
  }

  set: detail {
    fields: [
      filename,
      start_time,
      end_time,
      total,
      success,
      failure,
      sf_last_seeen,
      sf_count,
      athena_last_seen,
      athena_count
    ]
  }
}
