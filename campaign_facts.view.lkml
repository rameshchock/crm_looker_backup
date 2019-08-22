view: sf_campaign_facts {
  sql_table_name: LOOKER.CAMPAIGN_FACTS ;;

  dimension_group: assembly_time {
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
    sql: ${TABLE}."ASSEMBLY_TIME" ;;
  }

  dimension: bounce_reason_key {
    type: string
    sql: ${TABLE}."BOUNCE_REASON_KEY" ;;
  }

  measure: bounced {
    type: sum
    sql: ${TABLE}."BOUNCED" ;;
  }



  measure: bounced_blocked {
    type: sum
    sql: ${TABLE}."BOUNCED_BLOCKED" ;;
  }

  measure: bounced_hard {
    type: sum
    sql: ${TABLE}."BOUNCED_HARD" ;;
  }

  measure: bounced_soft {
    type: sum
    sql: ${TABLE}."BOUNCED_SOFT" ;;
  }

  dimension: campaign_id {
    type: string
    sql: ${TABLE}."CAMPAIGN_ID" ;;
  }

  dimension: campaign_name {
    type: string
    sql: ${TABLE}."CAMPAIGN_NAME" ;;
  }

  dimension: campaign_type {
    type: string
    sql: ${TABLE}."CAMPAIGN_TYPE" ;;
  }

  dimension: channel {
    type: string
    sql: ${TABLE}."CHANNEL" ;;
  }

  measure: clicked {
    type: sum
    sql: ${TABLE}."CLICKED" ;;
  }

  measure: complained {
    type: sum
    sql: ${TABLE}."COMPLAINED" ;;
  }

  measure: converted {
    type: sum
    sql: ${TABLE}."CONVERTED" ;;
  }

  measure: delivered {
    type: sum
    sql: ${TABLE}."DELIVERED" ;;
  }

  dimension_group: delivered_time {
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
    sql: ${TABLE}."DELIVERED_TIME" ;;
  }

  measure: delivery_lagsecs {
    type: sum
    sql: ${TABLE}."DELIVERY_LAGSECS" ;;
  }

  dimension: message_uid {
    type: string
    sql: ${TABLE}."MESSAGE_UID" ;;
  }

  measure: opened {
    type: sum
    sql: ${TABLE}."OPENED" ;;
  }

  measure: queued_secs {
    type: sum
    sql: ${TABLE}."QUEUED_SECS" ;;
  }

  dimension: recurrence_index {
    type: string
    sql: ${TABLE}."RECURRENCE_INDEX" ;;
  }

  dimension_group: send_time {
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
    sql: ${TABLE}."SEND_TIME" ;;
  }

  measure: sent {
    type: sum
    sql: ${TABLE}."SENT" ;;
  }

  dimension: site_id {
    type: string
    sql: ${TABLE}."SITE_ID" ;;
  }

  dimension: template_id {
    type: string
    sql: ${TABLE}."TEMPLATE_ID" ;;
  }

  measure: total_clicks {
    type: sum
    sql: ${TABLE}."TOTAL_CLICKS" ;;
  }

  measure: total_opens {
    type: sum
    sql: ${TABLE}."TOTAL_OPENS" ;;
  }

  measure: unsubscribed {
    type: sum
    sql: ${TABLE}."UNSUBSCRIBED" ;;
  }

  dimension: variation_sent {
    type: string
    sql: ${TABLE}."VARIATION_SENT" ;;
  }

  dimension: variation_testing_enabled {
    type: yesno
    sql: ${TABLE}."VARIATION_TESTING_ENABLED" ;;
  }

  measure: count {
    type: count
    drill_fields: [campaign_name]
  }
}
