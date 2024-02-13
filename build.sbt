ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.12"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % "test"
libraryDependencies += "com.softwaremill.sttp.client4" %% "core" % "4.0.0-M1"
libraryDependencies += "com.lihaoyi" %% "ujson" % "3.0.0"

// https://mavenlibs.com/maven/dependency/ch.hsr/geohash
libraryDependencies += "ch.hsr" % "geohash" % "1.4.0"
lazy val root = (project in file("."))
  .settings(
    name := "SparkBD"
  )
