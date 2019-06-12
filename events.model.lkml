connection: "rtem"

# include all the views
include: "*.view"

datagroup: events_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: events_default_datagroup

explore: aggr_site_id_daily {}

explore: aggr_site_id_hourly {}

explore: aggr_siteid_event_types_daily {}

explore: aggr_siteid_event_types_hourly {}

#-- explore: daily_partition_log_manager {}

#-- explore: hourly_partition_log_manager {}

#-- explore: show_partitions {}

#-- explore: site_id {}

#-- explore: site_id_by_min {}

#-- explore: siteid_event_types {}

#-- explore: siteid_event_types_by_min {}

explore: aggr_campaigns_daily {}

explore: aggr_campaigns_hourly {}
