addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.13")

val protoBufVersion = "0.6.6"

val protoBufComiler = "com.trueaccord.scalapb" %% "compilerplugin"  % protoBufVersion
val protoBufRuntime = "com.trueaccord.scalapb" %% "scalapb-runtime" % protoBufVersion

libraryDependencies ++= Seq(
  protoBufComiler, protoBufRuntime
)