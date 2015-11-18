name := "db-realtime-changefeed"

version := "0.1"

scalaVersion := "2.11.7"

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-encoding", "UTF-8",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Xfuture",
  "-Ywarn-unused-import",
  "-feature"
)

val AkkaVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.mongodb.scala" %% "mongo-scala-driver" % "1.0.0",
  "com.github.nscala-time" %% "nscala-time" % "2.4.0",
  "com.typesafe.akka" %% "akka-actor" % AkkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % "test",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.2" % "test"
)
    