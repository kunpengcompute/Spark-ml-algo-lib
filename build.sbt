name := "kal-test"

version := "0.1"

scalaVersion := "2.11.8"


libraryDependencies += "org.json4s" %% "json4s-ext" % "3.2.11";
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.2" withSources();
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.3.2" withSources();
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.2" withSources();
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.3.2" withSources();
libraryDependencies += "org.scalanlp" % "breeze_2.11" % "0.11.2" withSources();
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.3" % "test";
libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test";
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.2.0";
libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0";
libraryDependencies += "it.unimi.dsi" % "fastutil" % "8.3.1";

