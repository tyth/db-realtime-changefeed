package org.db.changefeed

import java.util.UUID

import akka.actor.{PoisonPill, Props, ActorSystem}
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.db.changefeed.CollectionFeedActor.{Fetch, Increment}
import org.mongodb.scala.MongoDatabase

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext.Implicits.global

case class ChangeEvent[K, V](key: K, oldValue: Option[V], newValue: V)

case class Trigger[K, V](id: UUID, condition: (V, V) => Boolean, callback: ChangeEvent[K, V] => Unit)


/**
 * Facade over @CollectionFeedActor
 */
case class CollectionFeed[K: Ordering, V](collection: Collection[K, V], triggers: Seq[Trigger[K, V]], values: Map[K, V])
                                         (implicit num: Numeric[V], system: ActorSystem) extends AutoCloseable {
  val actor = system.actorOf(Props(new CollectionFeedActor(collection, triggers, values)))
  implicit val timeout = Timeout(10.seconds)

  /**
   * Increments value of given key, returns UUID list of activated triggers.
   */
  def increment(key: K, incrementValue: V = num.one): Future[Seq[UUID]] = {
    (actor ? Increment(key, incrementValue)).mapTo[Seq[UUID]]
  }

  /**
   * Returns sorted sequence of key-value pairs
   */
  def fetch(): Future[Seq[(K, V)]] = {
    (actor ? Fetch).mapTo[Seq[(K, V)]]
  }

  override def close(): Unit = actor ! PoisonPill
}

case class MongoCollectionFeedBuilder[K: Ordering, V](name: String, database: MongoDatabase)
                                                     (implicit dbKey: MongoDbKey[K], dbValue: MongoDbValue[V],
                                                      num: Numeric[V], actorSystem: ActorSystem) {
  val triggers = mutable.ArrayBuffer[Trigger[K, V]]()

  val collection = database.getCollection(name)

  def build: Future[CollectionFeed[K, V]] = {
    collection.find().toFuture().map { docs =>
      println("fetched" + docs.toString)

      val lastFetchedValues = Map[K, V]() ++
        docs.map(d => d.get("key") -> d.get("value")).collect { case (Some(key), Some(value)) =>
          dbKey.read(key) -> dbValue.read(value)
        }
      val coll = new MongoDbCollection[K, V](collection)
      CollectionFeed(coll, triggers.toSeq, lastFetchedValues)
    }
  }

  def addTrigger(condition: (V, V) => Boolean, callback: ChangeEvent[K, V] => Unit): MongoCollectionFeedBuilder[K, V] = {
    addTrigger(Trigger(UUID.randomUUID(), condition, callback))
  }

  def addTrigger(trigger: Trigger[K, V]) = {
    triggers.append(trigger)
    this
  }
}

/**
 * Starting point for constructing change feeds.
 */
case class MongoDbChangefeed(database: MongoDatabase) {
  def collection[K: Ordering, V](name: String)
                                (implicit dbKey: MongoDbKey[K], dbValue: MongoDbValue[V], num: Numeric[V],
                                 actorSystem: ActorSystem = ActorSystem(s"changefeed-system-$name", MongoDbChangefeed.defaultAkkaConfig)) = {
    MongoCollectionFeedBuilder[K, V](name, database)
  }
}

object MongoDbChangefeed {
  val defaultAkkaConfig = ConfigFactory.parseString("akka.daemonic=on")
}