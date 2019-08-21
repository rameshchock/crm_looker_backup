view: page_response_stats {
  sql_table_name: process_metrics.v_page_response_stats ;;

  dimension_group: report_date {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.report_date ;;
  }


  dimension: avg_response_time_val {
    type: number
    sql: ${TABLE}.avg_response_time ;;
  }

  dimension: max_response_time_val {
    type: number
    sql: ${TABLE}.max_response_time ;;
  }

  dimension: min_reponse_time_val {
    type: number
    sql: ${TABLE}.min_reponse_time ;;
  }

  dimension: page {
    type: string
    sql: ${TABLE}.page ;;
  }

  dimension: section {
    type: string
    sql: ${TABLE}.section ;;
  }

  dimension_group: served {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.served_time ;;
  }


  measure: avg_response_time_milliseconds {
    type: average
    sql: ${TABLE}.avg_response_time ;;
    value_format:"#0"
  }

  measure: max_response_time_milliseconds {
    type: average
    sql: ${TABLE}.max_response_time ;;
    value_format:"#0"
  }

  measure: min_reponse_time_milliseconds {
    type: average
    sql: ${TABLE}.min_reponse_time ;;
    value_format:"#0"
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
