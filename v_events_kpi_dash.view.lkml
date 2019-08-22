view: v_events_kpi_dash {
  sql_table_name: site_event_aggregates.v_events_kpi_dash ;;

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: Metric {
    type: string
    sql: ${TABLE}.metric ;;
  }

  measure: This_Week {
    type: max
    sql: ${TABLE}.this_week ;;
  }

  measure: Last_Week {
    type: max
    sql: ${TABLE}.last_week ;;
  }

  measure: Current_MTD {
    type: max
    sql: ${TABLE}.current_mtd ;;
  }

  measure: Previous_Month {
    type: max
    sql: ${TABLE}.previous_month ;;
  }

  measure: Two_months_before {
    type: max
    sql: ${TABLE}.two_months_before ;;
  }

  measure: count {
    type: count

  }
}
