name := "akka-stream-java8"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.4.6",
  "com.typesafe.akka" % "akka-kernel_2.11" % "2.4.8",
    // https://mvnrepository.com/artifact/org.mongodb/mongo-java-driver
   "org.mongodb" % "mongo-java-driver" % "2.10.1",
   "org.bitbucket.dollar" % "dollar" % "1.0-beta2"
)

fork in run := true
