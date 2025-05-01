resource "docker_image" "zookeeper" {
  name = "confluentinc/cp-zookeeper:7.4.0"
}

resource "docker_container" "zookeeper" {
  name  = "zookeeper"
  image = docker_image.zookeeper.latest
  networks_advanced {
    name = docker_network.pipeline_network.name
  }
  ports {
    internal = 2181
    external = 2181
  }
  env = [
    "ZOOKEEPER_CLIENT_PORT=2181",
    "ZOOKEEPER_TICK_TIME=2000"
  ]
}

resource "docker_image" "kafka" {
  name = "confluentinc/cp-kafka:7.4.0"
}

resource "docker_container" "kafka" {
  name  = "kafka"
  image = docker_image.kafka.latest
  networks_advanced {
    name = docker_network.pipeline_network.name
  }
  ports {
    internal = 9092
    external = 9092
  }
  env = [
    "KAFKA_BROKER_ID=1",
    "KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
    "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092",
    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT",
    "KAFKA_AUTO_CREATE_TOPICS_ENABLE=true"
  ]
  depends_on = [docker_container.zookeeper]
}
