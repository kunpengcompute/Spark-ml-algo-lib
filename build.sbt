
lazy val commonSettings = Seq(
  scalaVersion := "2.11.8",
  parallelExecution in Test := false,

  libraryDependencies += "org.json4s" %% "json4s-ext" % "3.2.11",
  libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.2" withSources(),
  libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.3.2" withSources(),
  libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.2" withSources(),
  libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.3.2" withSources(),
  libraryDependencies += "org.scalanlp" % "breeze_2.11" % "0.11.2" withSources(),
  libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.3" % "test",
  libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test",
  libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.2.0",
  libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0",
  libraryDependencies += "it.unimi.dsi" % "fastutil" % "8.3.1"

)

lazy val mlcore = (project in file("ml-core"))
  .settings(commonSettings: _*)
  .settings(
    name := "sophon-ml-core",
    version := "1.0.0"
  )

lazy val mlacc = (project in file("ml-accelerator"))
  .dependsOn(mlcore % "compile->compile;test->test")
  .dependsOn(mlkernel % "compile->compile;test->test")
  .settings(commonSettings: _*)
  .settings(
    name := "sophon-ml-acc",
    version := "1.0.0"
  )

lazy val mlkernel = (project in file("ml-kernel"))
  .dependsOn(mlcore % "compile->compile;test->test")
  .settings(commonSettings: _*)
  .settings(
    name := "sophon-ml-kernel-client",
    version := "1.0.0"
  )







