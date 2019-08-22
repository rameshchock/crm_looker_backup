view: monthly_cost_by_site {
  derived_table: {
    sql: select active_sites.month_id, active_site_count, aws_cost
        from
        (
        select
        DATE_TRUNC('month', events_date ) month_id,
        count(distinct (case when (
        lower(site_id) like '%demo%'
        or lower(site_id) like '%test%'
        or lower(site_id) like '%dev%') then null else site_id end)) active_site_count
        from site_event_aggregates.aggr_site_id_daily
        group by 1
        having sum(events_count)>0
        )
        active_sites
        left join
        (
        select
        DATE_TRUNC('month', lineitem_usagestartdate ) month_id,sum(lineitem_blendedcost)* 0.913 aws_cost
        from
          aws_cost.agg_aws_cost  cr
        where
        cr.lineitem_usageaccountid IN ('066377602118','766806801073','636347950933','392159810864','947470032858')
         group by 1
         )
         monthly_zh_cost
         on active_sites.month_id=monthly_zh_cost.month_id
         where active_sites.month_id>='2019-02-01'
         and active_sites.month_id<DATE_TRUNC('month', current_date );;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: month_id {
    type: time
    timeframes: [
      raw,
      date,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.month_id ;;
  }

  dimension: month_id {
    type: string
    sql: ${TABLE}.month_id ;;
  }


  measure: active_sites {
    type: sum
    sql: ${TABLE}.active_site_count ;;
  }

  measure: aws_cost {
    type: sum
    sql: ${TABLE}.aws_cost;;
    value_format_name: usd_0

  }

  set: detail {
    fields: [month_id, active_sites, aws_cost]
  }
}
