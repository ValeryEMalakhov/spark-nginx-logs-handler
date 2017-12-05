import Dependencies._

name := "LogsEnricher"

version := "1.0.0.f2"

scalaVersion := scala211

mainClass in (Compile, run) := Some("com.prb.dnhs.Main")
scalaSource in Test := baseDirectory.value / "src" / "test" / "scala"

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

libraryDependencies ++= Seq(
  configType, scopt,
  hadoopCommon, hadoopClient,
  sparkCore, sparkSQL,
  specs2
)
