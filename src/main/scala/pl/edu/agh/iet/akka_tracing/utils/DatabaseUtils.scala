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
  private val logger = LoggerFactory.getLogger(getClass)

  private[akka_tracing] val couchDbClient = CouchDbClient(dbConfig)

  private val ReceiverMessagesDbName = "receiver_messages"
  private val SenderMessagesDbName = "sender_messages"
  private val MessagesRelationsDbName = "messages_relations"

  private[akka_tracing] val receiverMessagesDatabase = couchDbClient.getDb(ReceiverMessagesDbName)
  private[akka_tracing] val senderMessagesDatabase = couchDbClient.getDb(SenderMessagesDbName)
  private[akka_tracing] val messagesRelationsDatabase = couchDbClient.getDb(MessagesRelationsDbName)

  def init: Future[Unit] = {
    logger.info("Creating databases (if necessary)...")
    Future.sequence(Seq(
      senderMessagesDatabase.attemptToCreate,
      receiverMessagesDatabase.attemptToCreate,
      messagesRelationsDatabase.attemptToCreate
    )) flatMap {
      _ => replicateFromConfig()
    } map {
      _ => logger.info("Done")
    }
  }

  def clean: Future[Unit] = {
    logger.info("Cleaning databases...")
    Future.sequence(Seq(
      senderMessagesDatabase.deleteAllDocs(),
      receiverMessagesDatabase.deleteAllDocs(),
      messagesRelationsDatabase.deleteAllDocs()
    )) flatMap { _ =>
      if (dbConfig.getOrElse[Boolean]("compactOnDelete", false)) {
        Future.sequence(Seq(
          senderMessagesDatabase.compactDb(),
          receiverMessagesDatabase.compactDb(),
          messagesRelationsDatabase.compactDb()
        )) map { _ => () }
      } else {
        Future.successful(())
      }
    } map { _ => logger.info("Done") }
  }

  private def replicateFromConfig(): Future[Seq[Unit]] = {
    val replicationConfigOption = dbConfig.getOption[Config]("replication")
    replicationConfigOption map { replicationConfig =>
      val dbNames = Seq(
        ReceiverMessagesDbName,
        SenderMessagesDbName,
        MessagesRelationsDbName
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
      val replications = targets map {
        case (continuous, createDb, target) =>
        val protocol =
          if (target.useHttps) {
            "https"
          } else {
            "http"
          }
        logger.info(
          s"Performing replication from ${target.database} to " +
            s"$protocol://${target.host}:${target.port}/${target.database}"
        )
        ReplicationRequest(
          LocalReplicationDbConfig(target.database),
          target,
          continuous,
          createDb
        )
      }
      Future.sequence(replications.map(replication => couchDbClient.replicate(replication)))
    } getOrElse Future.sequence(Seq[Future[Unit]]())
  }
}
