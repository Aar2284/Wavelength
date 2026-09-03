name := "music-discovery-engine-scala"
version := "1.0"
scalaVersion := "2.13.18"

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",
  "com.github.graphframes" %% "graphframes" % "0.8.2-spark3.5-s_2.13"
)

fork := true
