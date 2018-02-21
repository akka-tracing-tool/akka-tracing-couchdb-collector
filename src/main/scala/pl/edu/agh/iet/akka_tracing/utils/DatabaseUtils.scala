package pl.edu.agh.iet.akka_tracing.utils

import com.typesafe.config.{ Config, ConfigFactory }
import org.slf4j.LoggerFactory
import pl.edu.agh.iet.akka_tracing.config.ConfigUtils
import pl.edu.agh.iet.akka_tracing.couchdb.CouchDbClient
import pl.edu.agh.iet.akka_tracing.couchdb.model._

import scala.concurrent.{ ExecutionContext, Future }

class DatabaseUtils(config: Config)(implicit ec: ExecutionContext) {

  import ConfigUtils._

  private val dbConfig = config.getOrElse[Config]("database", ConfigFactory.empty("database"))
  private val httpConfig = config.getOrElse[Config]("http", ConfigFactory.empty("http"))
  private val logger = LoggerFactory.getLogger(getClass)

  private[akka_tracing] val couchDbClient = CouchDbClient(dbConfig, httpConfig)

  private val UpdatesDbName = "updates"
  private[akka_tracing] val updatesDatabase = couchDbClient.getDb(UpdatesDbName)

  def init: Future[Unit] = {
    logger.info("Creating databases (if necessary)...")
    updatesDatabase.attemptToCreate flatMap { _ =>
      logger.info("Done")
      logger.info("Setting up replication...")
      replicateFromConfig()
    } map { _ => logger.info("Done") }
  }

  def clean: Future[Unit] = {
    logger.info("Cleaning databases...")
    updatesDatabase.deleteAllDocs() flatMap { _ =>
      if (dbConfig.getOrElse[Boolean]("compactOnDelete", false)) {
        logger.info("Done")
        logger.info("Compacting databases...")
        updatesDatabase.compactDb()
      } else {
        Future.successful(())
      }
    } map { _ => logger.info("Done") }
  }

  private def replicateFromConfig(): Future[Seq[Unit]] = {
    val replicationConfigOption = dbConfig.getOption[Config]("replication")
    replicationConfigOption map { replicationConfig =>
      val dbNames = Seq(
        UpdatesDbName
      )
      val targetsConfig = replicationConfig.getOrElse[Seq[Config]]("targets", Seq())
      val targets = targetsConfig.flatMap(targetConfig => {
        val host = targetConfig.getOrElse[String]("host", "localhost")
        val useHttps = targetConfig.getOrElse[Boolean]("useHttps", true)
        val port = targetConfig.getOrElse[Int]("port", if (useHttps) {
          5984
        } else {
          6984
        })
        val user = targetConfig.getOption[String]("user")
        val password = targetConfig.getOption[String]("password")
        val continuous = targetConfig.getOrElse[Boolean]("continuous", true)
        val createDb = targetConfig.getOrElse[Boolean]("createDb", false)
        dbNames.map(dbName => (continuous, createDb,
          RemoteReplicationDbConfig(host, port, dbName, useHttps, user, password)))
      })
      val replications = targets map { case (continuous, createDb, target) =>
        val protocol =
          if (target.useHttps) {
            "https"
          } else {
            "http"
          }
        logger.info(
          s"Performing replication from ${ target.database } to " +
            s"$protocol://${ target.host }:${ target.port }/${ target.database }"
        )
        ReplicationRequest(
          LocalReplicationDbConfig(target.database),
          target,
          continuous,
          createDb
        )
      }
      Future.sequence(replications.map(replication => couchDbClient.replicate(replication)))
    } getOrElse Future.successful(Seq.empty[Unit])
  }
}
