import Dependencies._
import sbt.Keys._

lazy val enricher = (project in file("."))
  .settings(
    name := "LogsEnricher",
    version := "1.0.0.f4",
    scalaVersion := scala211,
    mainClass in assembly := Some("com.prb.dnhs.MainApp"),
    mainClass in(Compile, run) := Some("com.prb.dnhs.MainApp"),
    fullClasspath in Runtime := (fullClasspath in Compile).value,
    test in assembly := {}
  )

exportJars := true
fork := true

libraryDependencies ++= Seq(
  sparkCore, sparkSQL, sparkStreaming, sparkHive,
  scalazStream, scalazCore,
  parquetColumn,
  configType, scopt,
  specs2, specs2Mock, slf4j, cats
)

scalacOptions += "-Ypartial-unification"

enablePlugins(AssemblyPlugin)

// Assembly added for correct work with scopt, slf4j and configType.
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_ *) => MergeStrategy.discard
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case x => MergeStrategy.last
}

assemblyJarName := "LogsEnricher.jar"

