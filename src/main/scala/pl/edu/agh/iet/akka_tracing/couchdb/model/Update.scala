package pl.edu.agh.iet.akka_tracing.couchdb.model

import pl.edu.agh.iet.akka_tracing.model.{ MessagesRelation, ReceiverMessage, SenderMessage }

case class Update(
    _id: String,
    receiverMessages: List[ReceiverMessage],
    senderMessages: List[SenderMessage],
    relationMessages: List[MessagesRelation],
    _rev: Option[String] = None
) extends Document
