module "sigma_container_service_nebulad" {
  source = "https://nexus.pkg.dev.vm.co.mz/repository/zip-public/tf-modules/sigma-container-service/1.1.7-RELEASE.zip"
  name   = var.name

  container = {
    image = var.app_docker_image
    listeners = [
      {
        name     = "cache"
        port     = 17171
        protocol = "TCP"
      },
      {
        name     = "admin-console"
        port     = 17172
        protocol = "TCP"
      },
      {
        name     = "metrics"
        port     = 17173
        protocol = "TCP"
      }
    ]
  }

  compute_resource_limit = {
    cpu       = 0.25
    memory_mb = 256
  }
}
