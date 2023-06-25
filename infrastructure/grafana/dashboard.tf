resource "grafana_dashboard" "metrics" {
    config_json = file("dashboard.json")
}
