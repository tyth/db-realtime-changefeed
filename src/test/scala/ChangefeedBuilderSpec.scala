/*package org.db.changefeed

import akka.actor.{Scheduler, ActorRef, Props, ActorSystem}
import org.db.changefeed.CollectionFeedActor.CleanUp
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.{FindObservable, MongoCollection, MongoDatabase}
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.Future
import scala.concurrent.duration._

class ChangefeedBuilderSpec extends WordSpecLike with Matchers with ScalaFutures with MockFactory {
  "MongoDbChangefeed" should {
    "build simplest changefeed with defaults" in {
      val dbMock = mock[MongoDatabase]
      val dbCollectionMock = mock[MongoCollection[Document]]
      val observableMock = mock[FindObservable[Document]]
      val actorSystemMock = mock[ActorSystem]
      val schedulerMock = mock[Scheduler]
      val actorMock = mock[ActorRef]
      val feedBuilder = MongoDbChangefeed(dbMock).collection[Long, Long]("metrics")


      (observableMock.toFuture _).expects().returning(Future successful Seq.empty)
      (dbMock.getCollection _).expects(*).returning(dbCollectionMock)
      (dbCollectionMock.find[Document](_)(_, _)).expects(*, *, *).returning(observableMock)

      (actorSystemMock.actorOf _).expects(*).returning(actorMock)
      (actorSystemMock.scheduler _).expects().returning(schedulerMock)
      (schedulerMock.schedule _).expects(1.minute, 1.minute, actorMock, CleanUp)
    }
  }
}
*/