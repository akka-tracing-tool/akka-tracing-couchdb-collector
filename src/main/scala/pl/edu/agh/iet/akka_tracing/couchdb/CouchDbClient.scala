package pl.edu.agh.iet.akka_tracing.couchdb

import com.typesafe.config.Config
import org.asynchttpclient.{ DefaultAsyncHttpClient, DefaultAsyncHttpClientConfig }
import org.json4s._
import pl.edu.agh.iet.akka_tracing.config.ConfigUtils
import pl.edu.agh.iet.akka_tracing.couchdb.model.ReplicationRequest

import scala.concurrent.{ ExecutionContext, Future }

class CouchDbClient(host: String,
                    port: Int,
                    useHttps: Boolean,
                    user: Option[String],
                    password: Option[String],
                    connectionTimeout: Int = 60000,
                    requestTimeout: Int = 120000)
                   (implicit ec: ExecutionContext) {

  import CouchDbUtils._

  private[couchdb] val asyncHttpClient = new DefaultAsyncHttpClient(
    new DefaultAsyncHttpClientConfig.Builder()
      .setConnectTimeout(connectionTimeout)
      .setRequestTimeout(requestTimeout)
      .build()
  )
  private implicit val formats = DefaultFormats

  private val protocol =
    if (useHttps) {
      "https"
    } else {
      "http"
    }

  private[couchdb] val baseUrl = s"$protocol://$host:$port"

  def getDb(name: String): CouchDatabase =
    new CouchDatabase(baseUrl, name, asyncHttpClient, user, password)

  def replicate(replicationRequest: ReplicationRequest): Future[Unit] = {
    val bodyBase = JObject(
      "source" -> replicationRequest.source.toJSON,
      "target" -> replicationRequest.target.toJSON,
      "continuous" -> JBool(replicationRequest.continuous)
    )
    val body =
      if (replicationRequest.createDb) {
        bodyBase merge JObject("create_target" -> JBool(true))
      } else {
        bodyBase
      }
    val request = buildRequest(s"$baseUrl/_replicate", "POST", user, password,
      body = Some(body)
    )
    asyncHttpClient.executeRequest(request).toCompletableFuture.asScala.map(
      _ => ()
    )
  }

  def close(): Unit = asyncHttpClient.close()

}

object CouchDbClient {

  import ConfigUtils._

  def apply(config: Config)(implicit ec: ExecutionContext): CouchDbClient = {
    val host = config.getOrElse[String]("host", "localhost")
    val useHttps = config.getOrElse[Boolean]("useHttps", true)
    val port = config.getOrElse[Int]("port",
      if (useHttps) {
        6984
      } else {
        5984
      }
    )
    val user = config.getOption[String]("user")
    val password = config.getOption[String]("password")
    val connectionTimeout = config.getOption[Int]("connectionTimeout").getOrElse(60000)
    val requestTimeout = config.getOption[Int]("requestTimeout").getOrElse(120000)
    new CouchDbClient(host, port, useHttps, user, password, connectionTimeout, requestTimeout)
  }
}
