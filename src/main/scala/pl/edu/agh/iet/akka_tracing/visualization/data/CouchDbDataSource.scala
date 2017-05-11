package pl.edu.agh.iet.akka_tracing.visualization.data

import java.util.UUID

import com.typesafe.config.Config
import pl.edu.agh.iet.akka_tracing.couchdb.model._
import pl.edu.agh.iet.akka_tracing.model.{ Message, MessagesRelation }
import pl.edu.agh.iet.akka_tracing.utils.DatabaseUtils

import scala.concurrent.{ ExecutionContext, Future }

class CouchDbDataSource(config: Config)(implicit val ec: ExecutionContext) extends DataSource {

  private[akka_tracing] val databaseUtils = new DatabaseUtils(config)

  import databaseUtils._

  override def onStart: Future[Unit] = databaseUtils.init

  override def getMessages: Future[List[Message]] = {
    senderMessagesDatabase.getAllDocs[CouchDbSenderMessage] flatMap { senderMessages =>
      val ids = senderMessages map { msg => msg._id }
      receiverMessagesDatabase.getDocs[CouchDbReceiverMessage](ids) map { receiverMessages =>
        senderMessages map { senderMessage =>
          val receiverMessage = receiverMessages
            .find(_._id == senderMessage._id)
          Message(
            UUID.fromString(senderMessage._id),
            senderMessage.sender,
            receiverMessage.map(_.receiver),
            senderMessage.contents
          )
        }
      }
    }
  }

  override def getRelations: Future[List[MessagesRelation]] =
    messagesRelationsDatabase.getAllDocs[CouchDbMessagesRelation] map { relations =>
      relations.map { relation => relation.toMessagesRelation }
    }
}

class CouchDbDataSourceConstructor extends DataSourceConstructor {
  override def fromConfig(config: Config)(implicit ec: ExecutionContext): DataSource = {
    new CouchDbDataSource(config)
  }
}
