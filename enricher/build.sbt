import Dependencies._
import sbt.Keys._

lazy val commonSettings = Seq(
  name := "LogsEnricher",
  version := "1.0.0.f4",
  scalaVersion := "2.11.11",
  autoScalaLibrary := false
)

lazy val enricher = project.in(file("."))
  .enablePlugins(Nexus)
  .enablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(artifactSettings)
  .settings(assemblySettings)
  .settings(testSettings)
  .settings(dependencySettings)

addArtifact(artifact in (Compile, assembly), assembly)

lazy val artifactSettings = Seq(
  mainClass in(Compile, run) := Some("com.prb.dnhs.MainApp"),
  artifactName in(Compile, packageBin) := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
    s"${artifact.name}.${artifact.extension}"
  },
  fullClasspath in Runtime := (fullClasspath in Compile).value
)

lazy val assemblySettings = Seq (
  test in assembly := {},
  mainClass in assembly := Some("com.prb.dnhs.MainApp"),
  assemblyJarName in assembly := "logsenricher.jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs@_ *) => MergeStrategy.discard
    case n if n.endsWith(".conf") => MergeStrategy.concat
    case x => MergeStrategy.last
  },
  artifact in (Compile, assembly) := {
    val art = (artifact in (Compile, assembly)).value
    art.withClassifier(Some("assembly"))
  }
)

lazy val testSettings = Seq(
  exportJars := true,
  fork := true,
  artifactName in Test := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
    s"${artifact.name}-tests.${artifact.extension}"
  }
)

lazy val dependencySettings = Seq(
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

