# 1. Custom VPC Network
resource "google_compute_network" "vpc" {
  name                    = var.network_name
  project                 = var.project_id
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"
}

# 2. Private Workload Subnet with Private Google Access
resource "google_compute_subnetwork" "subnet" {
  name                     = "${var.network_name}-subnet-${var.region}"
  project                  = var.project_id
  region                   = var.region
  network                  = google_compute_network.vpc.id
  ip_cidr_range            = var.subnet_cidr
  private_ip_google_access = true

  # Secondary IP ranges pre-allocated for future GKE pod/service alias IPs
  secondary_ip_range {
    range_name    = "gke-pods"
    ip_cidr_range = "10.20.0.0/16"
  }

  secondary_ip_range {
    range_name    = "gke-services"
    ip_cidr_range = "10.30.0.0/20"
  }
}

# 3. Cloud Router for NAT Dynamic Routing
resource "google_compute_router" "router" {
  name    = "${var.network_name}-router-${var.region}"
  project = var.project_id
  region  = var.region
  network = google_compute_network.vpc.id
}

# 4. Cloud NAT for Egress-Only Internet Access
resource "google_compute_router_nat" "nat" {
  name                               = "${var.network_name}-nat-${var.region}"
  project                            = var.project_id
  router                             = google_compute_router.router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}