import sbt.Keys._
import sbt._

object Nexus extends AutoPlugin {

  // Nexus resolving

  val nexusUrl = System.getProperty("nexusUrl", "http://192.168.80.132:8083/nexus")
  val nexusPublishRepo = System.getProperty("nexusPublishRepo", "content/groups/public")

  val creds = Credentials(Path.userHome / ".sbt" / ".credentials")
  // val creds = Credentials("Sonatype Nexus Repository Manager", "192.168.80.132", "admin", "admin123")

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
  )
}

