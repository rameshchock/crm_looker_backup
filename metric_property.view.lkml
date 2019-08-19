view: metric_property {
  sql_table_name: public.metric_property ;;

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: category {
    type: string
    sql: ${TABLE}.category ;;
  }

  dimension: module_name {
    type: string
    sql: ${TABLE}.module_name ;;
  }

  dimension: property_name {
    type: string
    sql: ${TABLE}.property_name ;;
  }

  dimension: property_type {
    type: string
    sql: ${TABLE}.property_type ;;
  }

  measure: count {
    type: count
    drill_fields: [id, module_name, property_name]
  }
}
