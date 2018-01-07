package org.pasalab.automj.benchmark

/**
 * Created by wuxiaoqi on 18-1-6.
 */
object ExperimentConst {
  val classLoader = getClass().getClassLoader

  val DEFAULT_TABLES_FILE = classLoader.getResource("tables").getPath
  val DEFAULT_QUERIES_FILE = classLoader.getResource("queries").getPath
  val DEFAULT_CONFIG_FILE = classLoader.getResource("config").getPath

  val TABLES_FILE = "spark.automj.experiment.tablesFile"
  val QUERIES_FILE = "spark.automj.experiment.queriesFile"
  val CONFIG_FILE = "spark.automj.experiment.configFile"
}
