package org.pasalab.automj.benchmark

import java.io.File

import org.apache.spark.sql.DataFrame
import org.pasalab.automj.benchmark.ExecutionMode.ForeachResults

/**
 * Created by wuxiaoqi on 18-1-5.
 */
class AutoMjPerformance extends Benchmark {
  val INPUT_DIR = "spark.automj.experiment.input"
  // TODO: 需要增加变化因素?

  val joinTables: Seq[Table] = {
    val inputDir = currentConfiguration.sparkConf.get(INPUT_DIR)
    // TODO: 这个文件名要卸载配置文件里
    val names = Seq("a", "b", "c")
    val dfs: Seq[(String, DataFrame)] = names.map {
      case name =>
        (name, sqlContext.read.json(inputDir+File.separator+name))
    }
    dfs.map {
      case (name, df) =>
        // 这里必须先注册, 否则后续的sql会找不到表名
        df.createOrReplaceTempView(name)
        Table(name, df)
    }
  }

  val queries: Seq[Query] = {
    // TODO: 从配置文件里读sql
    val sqlText: Seq[(String, String)] = Seq(("line", "SELECT * FROM a,b,c where a.x=b.x && b.y=c.y"))
    sqlText.map {
      case (name, sql) =>
        Query(name = name, sqlText = sql, description = "", executionMode = ForeachResults)
    }
  }
}
