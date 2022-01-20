import ReleaseTransformations._

scalaVersion := "2.13.7"

val debeziumVersion = "1.8.0.Final"
val kafkaVersion = "2.8.1"
val scalaTestVersion = "3.2.10"

libraryDependencies += "io.debezium" % "debezium-core" % debeziumVersion % "provided"

libraryDependencies += "org.apache.kafka" % "connect-api" % kafkaVersion
libraryDependencies += "org.apache.kafka" % "connect-transforms" % kafkaVersion % "provided"

libraryDependencies += "org.scalactic" %% "scalactic" % scalaTestVersion % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % "test"

releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    setNextVersion,
    commitNextVersion,
    pushChanges,
)
