variable "name" {
  type    = string
  default = "nebulad"
}

variable "app_docker_image" {
  type = string
  default = "docker.io/dyotamo/nebulad:latest"
}
