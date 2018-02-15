import Dependencies._
import sbt.Keys._

lazy val enricher = project.in(file("."))
  .enablePlugins(ArtifactoryPlugin)
  .settings(
    name := "LogsEnricher",
    version := "1.0.0.f3",
    scalaVersion := "2.11.11"
  )
  .settings(
    mainClass in(Compile, run) := Some("com.prb.dnhs.MainApp"),
    fullClasspath in Runtime := (fullClasspath in Compile).value
  )
  .settings(
    libraryDependencies ++=
      Seq(
        sparkCore,
        sparkSQL,
        sparkStreaming,
        sparkHive,
        scalazStream,
        scalazCore,
        parquetColumn,
        configType,
        scopt,
        slf4j,
        cats
      )
  )

