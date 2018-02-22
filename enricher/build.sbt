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
    artifactName in (Compile, packageBin) := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
      s"${artifact.name}.${artifact.extension}"
    },
    fullClasspath in Runtime := (fullClasspath in Compile).value
  )
  .settings(
    exportJars := true,
    fork := true,
    artifactName in Test := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
      s"${artifact.name}-tests.${artifact.extension}"
    }
  )
  .settings(
    dependencyOverrides ++=
      json,
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

