view: daily_aws_projected_cost {
  sql_table_name: aws_cost.daily_aws_projected_cost ;;

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: projectiondate {
    type: time
    timeframes: [
      date
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.projection_date ;;
  }

  dimension_group: usagedate {
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
    sql: ${TABLE}.month_id ;;
  }

  dimension: month_year {
    type: string
    sql: ${TABLE}.month ;;
  }

  dimension: business_unit {
    type: string
    sql: ${TABLE}.business_unit ;;
  }

  dimension: account_name {
    type: string
    sql: ${TABLE}.account_name ;;
  }

  dimension: lineitem_usageaccountid {
    type: string
    sql: ${TABLE}.lineitem_usageaccountid ;;
  }

  measure: monthly_usage_cost {
    type: sum
    sql: ${TABLE}.monthly_usage_cost ;;
    value_format_name: usd_0
  }

  measure: usage_cost_per_day {
    type: sum
    sql: ${TABLE}.usage_cost_pd ;;
    value_format_name: usd_0
  }

  measure: monthly_ri_cost {
    type: sum
    sql: ${TABLE}.monthly_ri_cost ;;
    value_format_name: usd_0
  }

  measure: ri_cost_per_day {
    type: sum
    sql: ${TABLE}.ri_cost_pd ;;
    value_format_name: usd_0
  }

  measure: day_count {
    type: sum
    sql: ${TABLE}.day_count ;;
    value_format_name: usd_0
  }

  measure: mtd_cost {
    type: sum
    sql: ${TABLE}.mtd_cost ;;
    value_format_name: usd_0
  }

  measure: projected_cost_wo_refund {
    type: sum
    sql: ${TABLE}.projected_cost ;;
    value_format_name: usd_0
  }

  measure: projected_cost_with_refund {
    type: sum
    sql: ${TABLE}.projected_cost_wref ;;
    value_format_name: usd_0
  }

  measure: projected_refund {
    type: sum
    sql: ${TABLE}.projected_refund;;
    value_format_name: usd_0
  }

  set: detail {
    fields: [
      usagedate_date,
      usagedate_week,
      usagedate_month,
      account_name,
      lineitem_usageaccountid,
      monthly_usage_cost,
      usage_cost_per_day,
      monthly_ri_cost,
      ri_cost_per_day,
      day_count,
      mtd_cost,
      projected_cost_wo_refund,
      projected_cost_with_refund,
      projected_refund
    ]
  }
}
