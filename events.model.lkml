connection: "rtem"

week_start_day: wednesday

# include all the views
include: "*.view"

# include: "*.dashboard.lookml"

datagroup: events_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: events_default_datagroup

# explore: aggr_site_id_daily {}

explore: v_daily_event_cost {}

explore: v_campaign_events {}

#-- explore: v_daily_event_cost_new {}

explore: v_site_cost_by_day {}

explore: monthly_cost_by_site {}

# explore: aggr_site_id_hourly {}

explore: aggr_siteid_event_types_daily {}

# explore: aggr_siteid_event_types_hourly {}

#-- explore: daily_partition_log_manager {}

#-- explore: hourly_partition_log_manager {}

#-- explore: show_partitions {}

#-- explore: site_id {}

#-- explore: site_id_by_min {}

#-- explore: siteid_event_types {}

#-- explore: siteid_event_types_by_min {}

explore: aggr_campaigns_daily {}

# explore: aggr_campaigns_hourly {}

explore: agg_aws_cost {}

explore: agg_cost_and_event {}

explore: page_response_stats {}

# -- explore: v_kpi_test {}

explore: v_events_kpi_dash {}

explore: v_events_kpi_report{}

explore: v_events_kpi_report_by_period {}

explore: aws_projected_cost {}

explore: daily_aws_projected_cost {}

explore: service_uptime_report {}

explore: v_service_uptime_report_by_period {}

explore: v_agg_site_event_stats {}

#explore: v_site_event_stats {}
