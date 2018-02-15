import sbt.Keys._
import sbt._

object ArtifactoryPlugin extends AutoPlugin {

  // Artifactory resolving
  val artifactoryUser = "admin"
  val artifactoryPassword = "admin123"

  val artifactoryUrl = System.getProperty("artifactoryUrl", "http://192.168.80.132:8083/nexus")
  val artifactoryContextUrl = System.getProperty("artifactoryContextUrl", "192.168.80.132")
  val artifactoryPublishRepo = System.getProperty("artifactoryPublishRepo", "content/groups/public")

  val creds = Credentials("Sonatype Nexus Repository Manager", artifactoryContextUrl, artifactoryUser, artifactoryPassword)

  // Settings to push artifacts to artifactory (jars, zip, etc)
  override lazy val projectSettings = Seq(
    resolvers := Seq(
      Resolver.defaultLocal,
      Resolver.mavenLocal,
      "Nexus" at s"$artifactoryUrl/$artifactoryPublishRepo"
    ),
    credentials += creds,
    publishTo := {
      if (isSnapshot.value)
        Some("maven-snapshots" at s"$artifactoryUrl/content/repositories/snapshots")
      else
        Some("maven-releases" at s"$artifactoryUrl/content/repositories/releases")
    },

    publishMavenStyle := true // Enables publishing to maven repo
    // isSnapshot := true // to fix overwrite ivy xml issue
  )

  /*
    lazy val publishSettingsWithNexus3 = Seq(
      publishMavenStyle := true,
      credentials += Credentials("Sonatype Nexus Repository Manager", "192.168.80.132", "admin", "admin123"),
      resolvers += "Nexus" at "http://192.168.80.132:8082/repository/maven-public/",
      publishTo := {
        val nexus = "http://192.168.80.132:8082"
        if (isSnapshot.value)
          Some("maven-snapshots" at nexus + "/repository/maven-snapshots")
        else
          Some("maven-releases" at nexus + "/repository/maven-releases")
      }
    )
  */
}

