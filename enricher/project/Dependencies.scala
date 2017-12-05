import sbt._

object Dependencies {

  lazy val scala211 = "2.11.11"
  lazy val scala212 = "2.12.4"

  lazy val sparkVersion = "2.2.0"
  lazy val hadoopVersion = "2.9.0"

  lazy val typesafeVersion = "1.3.2"
  lazy val scoptVersion = "3.7.0"
  lazy val jacksonVersion = "2.9.2"

  lazy val specs2Version = "4.0.0"

  lazy val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
  lazy val sparkSQL = "org.apache.spark" %% "spark-sql" % sparkVersion

  lazy val hadoopCommon = "org.apache.hadoop" % "hadoop-common" % hadoopVersion
  lazy val hadoopClient = "org.apache.hadoop" % "hadoop-client" % hadoopVersion

  lazy val configType = "com.typesafe" % "config" % typesafeVersion

  lazy val scopt = "com.github.scopt" %% "scopt" % scoptVersion
  lazy val jackson = "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion

  lazy val specs2 = "org.specs2" %% "specs2-core" % specs2Version % "test"
}

