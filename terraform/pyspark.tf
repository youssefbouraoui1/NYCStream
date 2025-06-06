resource "docker_image" "pyspark_etl" {
  name  = "pyspark_etl"
  build {
    path = "${path.module}/../pyspark_etl"
    dockerfile = "${path.module}/../pyspark_etl/Dockerfile"
  }
}

resource "docker_container" "pyspark_etl" {
  name  = "pyspark_etl"
  image = docker_image.pyspark_etl.latest

  networks_advanced {
    name = docker_network.pipeline_network.name
  }

  env = [
    "KAFKA_BROKER=kafka:9092",
    "KAFKA_TOPIC=raw_incidents",
    "PG_URL=jdbc:postgresql://postgres:5432/NycTrafficStreamDatabase",
    "PG_USER=admin",
    "PG_PASSWORD=admin123"
  ]

  restart = "always"
}
