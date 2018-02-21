package pl.edu.agh.iet.akka_tracing.visualization.data

import com.typesafe.config.Config
import pl.edu.agh.iet.akka_tracing.couchdb.model.Update
import pl.edu.agh.iet.akka_tracing.model._
import pl.edu.agh.iet.akka_tracing.utils.DatabaseUtils

import scala.concurrent.{ ExecutionContext, Future }

class CouchDbDataSource(config: Config)(implicit val ec: ExecutionContext) extends DataSource {

  private[akka_tracing] val databaseUtils = new DatabaseUtils(config)

  import databaseUtils._

  override def onStart: Future[Unit] = databaseUtils.init

  override def getMessages: Future[List[Message]] = {
    getUpdates map { updates =>
      val senderMessages = updates.foldLeft(List.empty[SenderMessage]) {
        case (acc, update) => acc ++ update.senderMessages
      }
      val receiverMessages = updates.foldLeft(List.empty[ReceiverMessage]) {
        case (acc, update) => acc ++ update.receiverMessages
      }
      senderMessages map { senderMessage =>
        val receiverMessage = receiverMessages.find(_.id == senderMessage.id)
        Message(
          senderMessage.id,
          senderMessage.sender,
          receiverMessage.map(_.receiver),
          senderMessage.contents
        )
      }
    }
  }

  override def getRelations: Future[List[MessagesRelation]] = getUpdates map { updates =>
    updates.foldLeft(List.empty[MessagesRelation]) {
      case (acc, update) => acc ++ update.relationMessages
    }
  }

  private[akka_tracing] def getUpdates: Future[List[Update]] = updatesDatabase.getAllDocs[Update]
}

class CouchDbDataSourceConstructor extends DataSourceConstructor {
  override def fromConfig(config: Config)(implicit ec: ExecutionContext): DataSource = {
    new CouchDbDataSource(config)
  }
}
