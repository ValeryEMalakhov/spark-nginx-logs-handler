import Dependencies._
import sbt.Keys._

lazy val commonSettings = Seq(
  name := "LogsAggregator"
  , version := "1.0.0"
  , scalaVersion := "2.10.4"
  , autoScalaLibrary := false
)

lazy val aggregator = project.in(file("."))
  .settings(commonSettings: _*)
  .settings(dependencySettings)
  .enablePlugins(AssemblyPlugin)
  .settings(artifactSettings)
  .settings(assemblySettings)

addArtifact(artifact in(Compile, assembly), assembly)

resolvers ++= Seq(
  "Hadoop Releases" at "https://repository.cloudera.com/content/repositories/releases/",
  "Maven Releases" at "http://central.maven.org/maven2/",
  "Cloudera Logs" at "http://repo.spring.io/plugins-release/",
  "Cloudera Releases" at "https://repository.cloudera.com/artifactory/libs-release-local/"
)

lazy val artifactSettings = Seq(
  mainClass in(Compile, run) := Some("com.prb.agg.MainApp")
  , artifactName in(Compile, packageBin) := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
    s"${artifact.name}.${artifact.extension}"
  }
  , fullClasspath in Runtime := (fullClasspath in Compile).value
)

lazy val assemblySettings = Seq(
  test in assembly := {}
  , mainClass in assembly := Some("com.prb.agg.MainApp")
  , assemblyJarName in assembly := "logsaggregator.jar"
  , assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs@_ *) => MergeStrategy.discard
    case n if n.endsWith(".conf") => MergeStrategy.concat
    case x => MergeStrategy.last
  }
  , artifact in(Compile, assembly) := {
    val art = (artifact in(Compile, assembly)).value
    art.withClassifier(Some("assembly"))
  }
)

lazy val dependencySettings = Seq(
  dependencyOverrides ++=
    json ++ jetty ++ Seq(logredactor),
  libraryDependencies ++=
    spark ++
    hadoop ++
    parquet ++
    hbase ++
    scalaz ++
    sTest ++
    Seq(
        configType
      , scopt
      , slf4j
      , cats
    )
)