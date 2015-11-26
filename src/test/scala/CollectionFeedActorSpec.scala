package org.db.changefeed

import java.util.UUID

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{DefaultTimeout, TestActorRef, TestKit}
import org.db.changefeed.CollectionFeedActor.{CleanUp, Fetch, Increment}
import org.joda.time.DateTime
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.Future

class CollectionFeedActorSpec extends TestKit(ActorSystem()) with DefaultTimeout with WordSpecLike with Matchers
with ScalaFutures with MockFactory {

  import system.dispatcher

  trait Fixture extends CommonImplicits {
    val collection = mock[Collection[DateTime, Long]]

    def createActor(collection: Collection[DateTime, Long],
                    triggers: Seq[Trigger[DateTime, Long]] = Seq.empty,
                    values: Map[DateTime, Long] = Map.empty,
                    range: Option[KeyRange[DateTime]] = None) = {
      TestActorRef(new CollectionFeedActor(collection, triggers, values, range))
    }
  }

  "CollectionFeedActor" should {
    "increment value" in new Fixture {
      val actorRef = createActor(collection)
      val ts = DateTime.now
      (collection.increment _).expects(ts, 1l).returning(Future.successful(true)).once()

      whenReady((actorRef ? Increment(ts, 1l)).mapTo[Seq[UUID]]) { firedTriggers: Seq[UUID] =>
        firedTriggers shouldBe empty
        actorRef.underlyingActor.latestValues should have size 1
        actorRef.underlyingActor.latestValues.get(ts) shouldBe Some(1l)
      }
    }

    "trigger event on increment" in new Fixture {
      val ts = DateTime.now
      val callback = mockFunction[ChangeEvent[DateTime, Long], Unit]
      val trigger = Trigger[DateTime, Long](UUID.randomUUID(), (a, b) => math.abs(a - b) > 2, callback)
      val actorRef = createActor(collection, Seq(trigger))

      (collection.increment _).expects(*, *).returning(Future.successful(true)).repeated(3)
      callback.expects(ChangeEvent(ts, None, 1l))

      val resultF = for {
        _ <- actorRef ? Increment(ts, 1l)
        _ <- actorRef ? Increment(ts, 1l)
        result <- actorRef ? Increment(ts, 1l)
      } yield result

      whenReady(resultF) { _ =>
        actorRef.underlyingActor.latestValues should have size 1
        actorRef.underlyingActor.latestValues.get(ts) shouldBe Some(3l)
      }
    }

    "consider already existing values" in new Fixture {
      val ts = DateTime.now
      val callback = mockFunction[ChangeEvent[DateTime, Long], Unit]
      val trigger = Trigger[DateTime, Long](UUID.randomUUID(), (a, b) => math.abs(a - b) > 1, callback)
      val actorRef = createActor(collection, Seq(trigger), Map(ts -> 2l))

      (collection.increment _).expects(ts, 2l).returning(Future.successful(true))
      callback.expects(ChangeEvent(ts, Some(2l), 4l))

      whenReady((actorRef ? Increment(ts, 2l)).mapTo[Seq[UUID]]) { firedTriggers: Seq[UUID] =>
        firedTriggers shouldBe Seq(trigger.id)
        actorRef.underlyingActor.latestValues.get(ts) shouldBe Some(4l)
      }
    }

    "correctly trigger events for different conditions" in new Fixture {
      val ts1 = DateTime.now
      val ts2 = ts1.minusSeconds(10)
      val callback1 = mockFunction[ChangeEvent[DateTime, Long], Unit]
      val trigger1 = Trigger[DateTime, Long](UUID.randomUUID(), (a, b) => math.abs(a - b) > 1, callback1)
      val callback2 = mockFunction[ChangeEvent[DateTime, Long], Unit]
      val trigger2 = Trigger[DateTime, Long](UUID.randomUUID(), (a, b) => math.abs(a - b) > 5, callback2)
      val actorRef = createActor(collection, Seq(trigger1, trigger2))

      (collection.increment _).expects(*, *).returning(Future.successful(true)).repeated(4)

      inSequence {
        inAnyOrder {
          callback1.expects(ChangeEvent(ts1, None, 1l))
          callback2.expects(ChangeEvent(ts1, None, 1l))
        }
        callback1.expects(ChangeEvent(ts1, Some(1l), 3l))

        inAnyOrder {
          callback1.expects(ChangeEvent(ts1, Some(3l), 23l))
          callback2.expects(ChangeEvent(ts1, Some(1l), 23l))
        }

        callback1.expects(ChangeEvent(ts2, None, 1l))
        callback2.expects(ChangeEvent(ts2, None, 1l))
      }

      val resultF = for {
        _ <- actorRef ? Increment(ts1, 1l)
        _ <- actorRef ? Increment(ts1, 2l)
        _ <- actorRef ? Increment(ts1, 20l)
        result <- actorRef ? Increment(ts2, 1l)
      } yield result

      whenReady(resultF) { _ =>
        actorRef.underlyingActor.latestValues.get(ts1) shouldBe Some(23l)
        actorRef.underlyingActor.latestValues.get(ts2) shouldBe Some(1l)
      }
    }

    "fetch sorted values" in new Fixture {
      val ts1 = DateTime.now
      val ts2 = ts1.minusSeconds(10)
      val actorRef = createActor(collection)

      (collection.increment _).expects(*, *).returning(Future.successful(true)).repeat(3)

      val resultF = for {
        _ <- actorRef ? Increment(ts1, 1l)
        _ <- actorRef ? Increment(ts1, 2l)
        _ <- actorRef ? Increment(ts2, 1l)
        result <- actorRef ? Fetch
      } yield result

      whenReady(resultF.mapTo[Seq[(DateTime, Long)]]) { data =>
        data should have size 2
        data(0) should be(ts2 -> 1l)
        data(1) should be(ts1 -> 3l)
      }
    }


    "clean up values and ignore increment outside of specified range" in new Fixture {
      val ts = DateTime.now
      val callback = mockFunction[ChangeEvent[DateTime, Long], Unit]
      val trigger = Trigger[DateTime, Long](UUID.randomUUID(), _ != _, callback)
      val range = KeyRange[DateTime](ts.minusSeconds(30), ts.plusSeconds(30))
      val actorRef = createActor(collection, Seq(trigger), Map(ts -> 1l, ts.minusSeconds(15) -> 1l, ts.plusMinutes(1) -> 1l), Some(range))
      (collection.increment _).expects(*, *).returning(Future.successful(true)).repeat(3)

      callback.expects(ChangeEvent(ts, Some(1l), 2l)).onCall { e: ChangeEvent[DateTime, Long] => println(e) }
      callback.expects(ChangeEvent(ts.plusSeconds(20), None, 1l))

      val resultF = for {
        _ <- actorRef ? Increment(ts, 1l)
        _ <- actorRef ? Increment(ts.plusSeconds(20), 1l)
        _ <- actorRef ? Increment(ts.plusSeconds(40), 1l)
        _ <- actorRef ? CleanUp
        result <- actorRef ? Fetch
      } yield result

      whenReady(resultF.mapTo[Seq[(DateTime, Long)]]) { data =>
        data should have size 3
        data(0) should be(ts.minusSeconds(15) -> 1l)
        data(1) should be(ts -> 2l)
        data(2) should be(ts.plusSeconds(20) -> 1l)

        val actor = actorRef.underlyingActor
        actor.latestValues should have size 3
        actor.lastFetchedValues.foreach { case (_, vals) =>
          vals should have size 3
        }
      }
    }
  }
}
