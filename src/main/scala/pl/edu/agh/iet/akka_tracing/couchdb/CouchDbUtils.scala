package pl.edu.agh.iet.akka_tracing.couchdb

import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.CompletableFuture

import org.asynchttpclient.{ Request, RequestBuilder, Response }
import org.json4s._
import org.json4s.native.JsonMethods._
import org.slf4j.LoggerFactory

import scala.compat.java8.FutureConverters._
import scala.concurrent.{ ExecutionContext, Future }

private[couchdb] object CouchDbUtils {
  private val base64Encoder = Base64.getEncoder
  private val logger = LoggerFactory.getLogger(getClass)

  private implicit val formats = DefaultFormats

  def getBase64AuthValue(user: String, password: String): String = {
    base64Encoder.encodeToString(
      s"$user:$password".getBytes(StandardCharsets.UTF_8)
    )
  }

  def buildRequest(url: String,
                   method: String,
                   userOption: Option[String],
                   passwordOption: Option[String],
                   queryParams: Option[List[(String, String)]] = None,
                   body: Option[JValue] = None): Request = {
    val rb = new RequestBuilder(method)
      .setUrl(url)
      .addHeader("Accept", "application/json")
    (userOption, passwordOption) match {
      case (Some(user), Some(password)) =>
        val encodedAuthString = getBase64AuthValue(user, password)
        rb.addHeader("Authorization", s"Basic $encodedAuthString")
      case _ =>
    }
    body foreach { json =>
      rb.addHeader("Content-type", "application/json")
        .setBody(compact(render(json)))
    }
    queryParams foreach { params =>
      params foreach { param =>
        rb.addQueryParam(param._1, param._2)
      }
    }
    rb.build()
  }

  implicit class RichCompletableFutureOfResponse(future: CompletableFuture[Response]) {
    def asScala(implicit ec: ExecutionContext): Future[Response] = {
      future.toScala flatMap { response =>
        val statusCode = response.getStatusCode
        if (statusCode < 200 || statusCode >= 300) {
          Future.failed(RequestFailedException(statusCode, response.getResponseBody(StandardCharsets.UTF_8)))
        }
        Future.successful(response)
      } recoverWith {
        case e@RequestFailedException(status, message) =>
          logger.error(s"Error while trying to perform request: $status: $message")
          Future.failed(e)
        case e =>
          logger.error("Error while trying to perform request:", e)
          Future.failed(e)
      }
    }
  }

  implicit class RichJValue(jValue: JValue) {
    def isDefined: Boolean = jValue.toOption.isDefined
  }

}
