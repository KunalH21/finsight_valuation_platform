output "provider_name" {
  value       = google_iam_workload_identity_pool_provider.github_provider.name
  description = "Full provider resource name used in GitHub Actions workflow step."
}

output "pool_name" {
  value       = google_iam_workload_identity_pool.github_pool.name
  description = "Full workload identity pool resource name."
}