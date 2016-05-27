name := """PlayReactiveMongoStrams"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  jdbc,
  cache,
  ws,
  "org.scalatestplus.play" %% "scalatestplus-play" % "1.5.1" % Test
)

libraryDependencies ++= Seq(
  "org.reactivemongo" %% "reactivemongo" % "0.11.11"
)

resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"
