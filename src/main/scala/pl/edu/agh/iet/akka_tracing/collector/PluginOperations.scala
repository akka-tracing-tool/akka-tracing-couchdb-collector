package pl.edu.agh.iet.akka_tracing.collector

import com.typesafe.config.ConfigFactory
import pl.edu.agh.iet.akka_tracing.utils.DatabaseUtils

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object PluginOperations extends App {
  if (!args.isEmpty) {
    val config = if (args.length > 1) {
      ConfigFactory.load(args(1))
    } else {
      ConfigFactory.load()
    }
    val databaseUtils = new DatabaseUtils(config.getConfig("akka_tracing.collector"))
    args.head match {
      case "init" =>
        Await.result(databaseUtils.init, Duration.Inf)
      case "clean" =>
        Await.result(databaseUtils.clean, Duration.Inf)
      case _ =>
    }
  }
}
