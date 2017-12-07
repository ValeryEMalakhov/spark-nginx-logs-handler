import Dependencies._

name := "LogsEnricher"

version := "1.0.0.f2"

scalaVersion := scala211

mainClass in (Compile, run) := Some("com.prb.dnhs.Main")

scalaSource in Test := baseDirectory.value / "src" / "test" / "scala"

libraryDependencies ++= Seq(
  configType, scopt,
  hadoopCommon, hadoopClient,
  sparkCore, sparkSQL,
  parquetHadoop, parquetColumn, parquetCommon,
  specs2
)
