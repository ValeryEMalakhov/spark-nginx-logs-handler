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
  .settings(publishSettingsWithNexus2)

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

lazy val publishSettingsWithNexus3 = Seq(
  publishMavenStyle := true,
  credentials += Credentials("Sonatype Nexus Repository Manager", "192.168.80.132", "admin", "admin123"),
  resolvers += "Nexus" at "http://192.168.80.132:8082/repository/maven-public/",
  publishTo := {
    val nexus = "http://192.168.80.132:8082"
    if (isSnapshot.value)
      Some("maven-snapshots" at nexus + "/repository/maven-snapshots")
    else
      Some("maven-releases" at nexus + "/repository/maven-releases")
  }
)

lazy val publishSettingsWithNexus2 = Seq(
  publishMavenStyle := true,
  credentials += Credentials("Sonatype Nexus Repository Manager", "192.168.80.132", "admin", "admin123"),
  resolvers += "Nexus" at "http://192.168.80.132:8083/nexus/content/groups/public",
  publishTo := {
    val nexus = "http://192.168.80.132:8083/nexus"
    if (isSnapshot.value)
      Some("maven-snapshots" at nexus + "/content/repositories/snapshots")
    else
      Some("maven-releases" at nexus + "/content/repositories/releases")
  }
)

