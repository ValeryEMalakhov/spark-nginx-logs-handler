import Dependencies._
import sbt.Keys._

lazy val enricher = project.in(file("."))
  .enablePlugins(Nexus)
  .settings(
    name := "LogsEnricher",
    version := "1.0.0.f4",
    scalaVersion := "2.11.11"
  )
  .settings(
    mainClass in(Compile, run) := Some("com.prb.dnhs.MainApp"),
    fullClasspath in Runtime := (fullClasspath in Compile).value,
    exportJars := true,
    fork := true
  )
  .settings(
    libraryDependencies ++=
      spark ++
      hadoop ++
      sTest ++
      Seq(
          scalazStream
        , scalazCore
        , parquetColumn
        , configType
        , scopt
        , slf4j
        , cats
      )
  )

