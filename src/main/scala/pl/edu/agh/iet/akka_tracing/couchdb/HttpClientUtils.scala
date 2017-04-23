package pl.edu.agh.iet.akka_tracing.couchdb

import java.nio.charset.Charset
import java.util.Base64
import java.util.concurrent.CompletableFuture

import org.asynchttpclient.{ Request, RequestBuilder, Response }

import scala.compat.java8.FutureConverters
import scala.concurrent.{ ExecutionContext, Future }

object HttpClientUtils {

  def buildRequest(url: String,
                   method: String,
                   userOption: Option[String],
                   passwordOption: Option[String],
                   queryParams: Option[List[(String, String)]] = None,
                   body: Option[String] = None): Request = {
    val rb = new RequestBuilder(method)
      .setUrl(url)
      .addHeader("Accept", "application/json")
    (userOption, passwordOption) match {
      case (Some(user), Some(password)) =>
        val encodedAuthString = Base64.getEncoder.encodeToString(
          s"$user:$password".getBytes(Charset.forName("utf-8"))
        )
        rb.addHeader("Authorization", s"Basic $encodedAuthString")
      case _ =>
    }
    body foreach { serializedJson =>
      rb.addHeader("Content-type", "application/json")
        .setBody(serializedJson)
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
      FutureConverters.toScala(future) flatMap { response =>
        val statusCode = response.getStatusCode
        if (statusCode < 200 || statusCode >= 300) {
          Future.failed(RequestFailedException(statusCode))
        }
        Future.successful(response)
      }
    }
  }

}
