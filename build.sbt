import sbt.Keys.libraryDependencies

Global / excludeLintKeys += idePackagePrefix
Global / excludeLintKeys += test / fork
Global / excludeLintKeys += run / mainClass

// Define the version of your project.
ThisBuild / version := "0.1.0"

// Define the Scala version to be used.
ThisBuild / scalaVersion := "2.13.10"

// Define the project name.
name := "SparkRandomWalk"

unmanagedBase := baseDirectory.value / "lib"

// Define library versions. Adjust the version numbers according to your needs.
val scalaTestVersion = "3.2.15"
val typeSafeConfigVersion = "1.4.2"
val logbackVersion = "1.3.11"
val sfl4sVersion = "2.0.7"
val graphVizVersion = "0.18.1"
val sparkVersion = "3.5.0"

// Define common dependencies shared across your project.
lazy val commonDependencies = Seq(
  "com.typesafe" % "config" % typeSafeConfigVersion, // Typesafe Config Library
  "ch.qos.logback" % "logback-classic" % logbackVersion, // Logback Classic Logger
  "org.slf4j" % "slf4j-api" % sfl4sVersion, // SLF4J API Module
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test, // ScalaTest for testing
  "org.apache.spark" %% "spark-core" % sparkVersion, // Spark Core
  "org.apache.spark" %% "spark-sql" % sparkVersion, // Spark SQL
  "org.apache.spark" %% "spark-graphx" % sparkVersion // Spark GraphX
)

// Define your project and its dependencies.
lazy val root = (project in file("."))
  .settings(
    libraryDependencies ++= commonDependencies // Adding common dependencies to your project
  )

// Define Scala Compiler options.
scalacOptions ++= Seq(
  "-deprecation", // Emit warning and location for usages of deprecated APIs
  "--explain-types", // Explain type errors in more detail
  "-feature", // Emit warning and location for usages of features that should be imported explicitly
  "-Ytasty-reader"
)

// Define JVM options for running your project.
//compileOrder := CompileOrder.JavaThenScala
//test / fork := true
//run / fork := true
run / javaOptions ++= Seq(
  "-Xms512M", // Initial JVM heap size
  "-Xmx2G", // Maximum JVM heap size
  "-XX:+UseG1GC" // Use G1 Garbage Collector
)

// Define the main class. Replace with the actual main class of your application.
Compile / mainClass := Some("com.lsc.Main")

val jarName = "RandomWalk.jar"
assembly/assemblyJarName := jarName

//Merging strategies
ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}
