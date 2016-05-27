package provider.properties

/**
  *
  */
case class MongodbProperties(hosts: List[Host], database: String, collection: String, username: Option[String] = None,
                             password: Option[String] = None) {
  def prettyPrint: String = {
    val hostsString = hosts.map(h => s"${h.name}:${h.port}").reduce(_ + ", " + _)
    s"Data source type: MongoDb. Hosts: $hostsString. Database: $database"
  }
}

case class Host(name: String, port: Int)