import Dependencies._
import sbt.Keys._

lazy val commonSettings = Seq(
  name := "LogsEnricher"
  , version := "1.0.0.f5"
  , scalaVersion := "2.10.4"
  , autoScalaLibrary := false
)

lazy val enricher = project.in(file("."))
  .settings(commonSettings: _*)
  .settings(dependencySettings)
  //.enablePlugins(Nexus)
  .enablePlugins(AssemblyPlugin)
  .enablePlugins(sbtdocker.DockerPlugin)
  .settings(artifactSettings)
  .settings(assemblySettings)
  .settings(dockerSettings)
  .settings(testSettings)
  .configs(FunTest)
  .settings(inConfig(FunTest)(Defaults.testSettings): _*)
  .settings(itSettings)

addArtifact(artifact in(Compile, assembly), assembly)

// scalacOptions += "-Ylog-classpath"

resolvers ++= Seq(
  "Hadoop Releases" at "https://repository.cloudera.com/content/repositories/releases/",
  "Maven Releases" at "http://central.maven.org/maven2/",
  "Cloudera Logs" at "http://repo.spring.io/plugins-release/",
  "Cloudera Releases" at "https://repository.cloudera.com/artifactory/libs-release-local/"
)

lazy val artifactSettings = Seq(
  mainClass in(Compile, run) := Some("com.prb.dnhs.MainApp")
  , artifactName in(Compile, packageBin) := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
    s"${artifact.name}.${artifact.extension}"
  }
  , fullClasspath in Runtime := (fullClasspath in Compile).value
)

lazy val assemblySettings = Seq(
  test in assembly := {}
  , mainClass in assembly := Some("com.prb.dnhs.MainApp")
  , assemblyJarName in assembly := "logsenricher.jar"
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

lazy val testSettings = Seq(
  exportJars := true
  , fork in Test := true
  , parallelExecution in Test := false
  , artifactName in Test := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
    s"${artifact.name}-tests.${artifact.extension}"
  }
  , scalaSource in Test := baseDirectory.value / "src/test/scala"
  //, resourceDirectory in Test := baseDirectory.value / "src/test/resources"
)

lazy val FunTest = config("it").extend(Test)

lazy val itSettings =
//inConfig(IntegrationTest)(Defaults.testSettings) ++
  Seq(
    fork in IntegrationTest := true
    , parallelExecution in IntegrationTest := false
    , artifactName in IntegrationTest := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
      s"${artifact.name}-it.${artifact.extension}"
    }
    , scalaSource in IntegrationTest := baseDirectory.value / "src/it/scala"
    //, resourceDirectory in IntegrationTest := baseDirectory.value / "src/it/resources"
  )

lazy val dockerSettings = Seq(
  // things the docker file generation depends on are listed here
  dockerfile in docker := {
    // any vals to be declared here
    new sbtdocker.mutable.Dockerfile {
      // The assembly task generates a fat JAR file
      val artifact: File = assembly.value
      val artifactTargetPath = s"/app/${artifact.name}"

      new Dockerfile {
        from("se_env")
        add(artifact, artifactTargetPath)
        //entryPoint("java", "-jar", artifactTargetPath)
      }
    }
  }
  , buildOptions in docker := BuildOptions(cache = false)
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