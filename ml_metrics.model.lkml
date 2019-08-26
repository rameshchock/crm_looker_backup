connection: "ml-metrics-dev"

# include all the views
include: "*.view"

datagroup: ml_metrics_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: ml_metrics_default_datagroup

explore: metric_events {}

explore: metric_property {}

explore: ftp_event_import_with_site_id {}
