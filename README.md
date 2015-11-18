Database realtime changefeed [![Build Status](https://travis-ci.org/tyth/db-realtime-changefeed.svg)](https://travis-ci.org/tyth/db-realtime-changefeed)
---

### Introduction

Note: This is a proof-of-concept project right now. WIP.

This project is intended as a backend for real-time dashboards, where updates are pushed from the server.
When there are thousands of metric changes per second, you don't want to push updates to a client for every small change.
`Trigger` allow you to specify a condition (e.g. _X_ changed for more than 10% from last notification time) on which an event will be fired.
A single ChangeFeed can contain multiple different triggers, so one can be used for regular data updates, and other for alerts notifications. Basic ChangeFeed works with a collection of items, whether it's a time series data, key-value collection or a single metric.

This project provides real-time change feed as a layer on top of existing database. Right now it supports only MongoDB storage.

### Usage

Restrictions: All updates has to come through one CollectionFeed.

```
import org.db.changefeed._
import MongoDbImplicits._

import org.mongodb.scala.MongoClient
import org.joda.time.DateTime

val db = MongoClient().getDatabase("mydb")
val feedBuilder = MongoDbChangefeed(db).collection[DateTime, Long]("metrics")

val condition = (oldVal: Long, newVal: Long) => math.abs(oldVal - newVal) > 2
val action = (event: ChangeEvent[DateTime, Long]) => println(s"Oh, new update: $event")

val feedF = feedBuilder.addTrigger(condition, action).build

feedF map { feed =>

  for {
    _ <- feed.increment(DateTime.now)
    _ <- feed.increment(DateTime.now, 2) // won't trigger any action yet
    _ <- feed.increment(DateTime.now, 1) // now our action is triggered

    _ <- feed.increment(DateTime.now.minusSeconds(1), 3)
    all <- feed.fetch()
  } yield all

}
```

If `MongoDbChangefeed` is not provided with ActorSystem as an implicit parameter, it creates a new one with daemonic threads.