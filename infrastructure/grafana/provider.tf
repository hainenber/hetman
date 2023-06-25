terraform {
  required_providers {
    grafana = {
      source = "grafana/grafana"
      version = "1.41.0"
    }
  }
}

provider "grafana" {
    url = "http://localhost:3000"
    auth = "anonymous"
}