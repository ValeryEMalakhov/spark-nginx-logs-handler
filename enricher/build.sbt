import Dependencies._

name := "LogsEnricher"

version := "1.0.0.f2"

scalaVersion := scala211

fork := true
enablePlugins(AssemblyPlugin)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_ *) => MergeStrategy.discard
  case x => MergeStrategy.last
}
test in assembly := {}

mainClass in assembly := Some("com.prb.dnhs.Main")
mainClass in (Compile, run) := Some("com.prb.dnhs.Main")

fullClasspath in Runtime := (fullClasspath in Compile).value

libraryDependencies ++= Seq(
  sparkCore, sparkSQL,
  configType, scopt,
  specs2, log4j
)

