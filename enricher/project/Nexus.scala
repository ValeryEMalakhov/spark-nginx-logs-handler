import sbt.Keys._
import sbt._

object Nexus extends AutoPlugin {

  // Nexus resolving
  val nexusUser = "admin"
  val nexusPassword = "admin123"

  val nexusUrl = System.getProperty("artifactoryUrl", "http://192.168.80.132:8083/nexus")
  val nexusContextUrl = System.getProperty("artifactoryContextUrl", "192.168.80.132")
  val nexusPublishRepo = System.getProperty("artifactoryPublishRepo", "content/groups/public")

  val creds = Credentials("Sonatype Nexus Repository Manager", nexusContextUrl, nexusUser, nexusPassword)

  // Settings to push artifacts to nexus (jars, zip, etc)
  override lazy val projectSettings = Seq(
    resolvers := Seq(
      Resolver.defaultLocal,
      Resolver.mavenLocal,
      "Nexus" at s"$nexusUrl/$nexusPublishRepo"
    ),
    credentials += creds,
    publishTo := {
      if (isSnapshot.value)
        Some("maven-snapshots" at s"$nexusUrl/content/repositories/snapshots")
      else
        Some("maven-releases" at s"$nexusUrl/content/repositories/releases")
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

