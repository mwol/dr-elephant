/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.spark.heuristics

import com.linkedin.drelephant.analysis.Severity
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ExecutorStageSummary, ExecutorSummary, StageData, StageDataImpl}
import com.linkedin.drelephant.analysis._
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.util.{MemoryFormatUtils, Utils}
import java.util.concurrent.TimeUnit

import org.apache.log4j.Logger

import scala.collection.JavaConverters

/**
  * A heuristic based on memory spilled.
  *
  */
class ExecutorStorageSpillHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData)
  extends Heuristic[SparkApplicationData] {

  import ExecutorStorageSpillHeuristic._
  import JavaConverters._
  private val logger: Logger = Logger.getLogger(classOf[ExecutorStorageSpillHeuristic])
  val sparkExecutorCoresThreshold : Int =
    if(heuristicConfigurationData.getParamMap.get(SPARK_EXECUTOR_CORES_THRESHOLD_KEY) == null) DEFAULT_SPARK_EXECUTOR_CORES_THRESHOLD
    else heuristicConfigurationData.getParamMap.get(SPARK_EXECUTOR_CORES_THRESHOLD_KEY).toInt

  val sparkExecutorMemoryThreshold : String =
    if(heuristicConfigurationData.getParamMap.get(SPARK_EXECUTOR_MEMORY_THRESHOLD_KEY) == null) DEFAULT_SPARK_EXECUTOR_MEMORY_THRESHOLD
    else heuristicConfigurationData.getParamMap.get(SPARK_EXECUTOR_MEMORY_THRESHOLD_KEY)

  val sparkExecutorMemoryThresholdForSpill :  String = "5GB"
  val sparkSqlPartitionCountThresholdForSpill: Int = 4000
  val taskCountThresholdSpill: Int = 4000


  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  def formatStagesWithSpillString(stagesWithSpill: Seq[Int]) : String = {
    if (stagesWithSpill.size != 0) {
      logger.info("Stages with Spill " + stagesWithSpill.mkString(","))
      "Stage " + stagesWithSpill.mkString(",")
    }
    else
      "None"
  }

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val stageAnalysisResult: Seq[StageAnalysis] = new StagesAnalyzer(heuristicConfigurationData, data).getStageAnalysis()
    logger.info("Stage Analysis Result for " + data.appId  + " " + stageAnalysisResult)
    val evaluator = new Evaluator(this, data, stageAnalysisResult)
    var resultDetails = Seq(new HeuristicResultDetails("Total memory spilled", MemoryFormatUtils.bytesToString(evaluator.totalMemorySpilled)),
      new HeuristicResultDetails("Max memory spilled", MemoryFormatUtils.bytesToString(evaluator.maxMemorySpilled)),
      new HeuristicResultDetails("Mean memory spilled", MemoryFormatUtils.bytesToString(evaluator.meanMemorySpilled)),
      new HeuristicResultDetails("Stages With Memory Spill", formatStagesWithSpillString(evaluator.stagesWithSpill))
    )

    if(evaluator.severity != Severity.NONE){
      resultDetails = resultDetails :+ new HeuristicResultDetails("Note", "Your execution memory is being spilled. Kindly look into it.")
      if(evaluator.sparkExecutorCores >= sparkExecutorCoresThreshold && evaluator.sparkExecutorMemory >= MemoryFormatUtils.stringToBytes(sparkExecutorMemoryThreshold)) {
        resultDetails = resultDetails :+ new HeuristicResultDetails("Recommendation", "You can try decreasing the number of cores to reduce the number of concurrently running tasks.")
      } else if (evaluator.sparkExecutorMemory <= MemoryFormatUtils.stringToBytes(sparkExecutorMemoryThreshold)) {
        resultDetails = resultDetails :+ new HeuristicResultDetails("Recommendation", "You can try increasing the executor memory to reduce spill.")
      }
      if (evaluator.partitionCount < sparkSqlPartitionCountThresholdForSpill || evaluator.sparkExecutorMemory < MemoryFormatUtils.stringToBytes(sparkExecutorMemoryThresholdForSpill)) {
        if (evaluator.taskSkewSeverity != Severity.NONE) {
          resultDetails = resultDetails :+ new HeuristicResultDetails("Data Skew", "Try to repartition the data as there is data skew present")
        }
        if (evaluator.totalTasks < taskCountThresholdSpill && evaluator.maxAmountOfDataProcessed >= MemoryFormatUtils.stringToBytes("1G")
          && TimeUnit.MILLISECONDS.toMinutes(evaluator.medianTaskRunTime) > 5) {
          resultDetails = resultDetails :+ new HeuristicResultDetails("Recommendation",
            " Suggested to increase the number of partitions using spark.default.parallelism for RDDs or spark.sql.shuffle.partitions if using DataFrames or DataSets")
        }
        if (evaluator.executorCores >= DEFAULT_SPARK_EXECUTOR_CORES_THRESHOLD) {
          resultDetails = resultDetails :+ new HeuristicResultDetails("Spark Executor Cores", "Suggested to decrease the spark.executor.cores and increase the number of executors")
        }
      }
    }
    val result = new HeuristicResult(
      heuristicConfigurationData.getClassName,
      heuristicConfigurationData.getHeuristicName,
      evaluator.severity,
      evaluator.score,
      resultDetails.asJava
    )
    result
  }
}

object ExecutorStorageSpillHeuristic {
  val SPARK_EXECUTOR_MEMORY = "spark.executor.memory"
  val SPARK_EXECUTOR_CORES = "spark.executor.cores"
  val SPARK_SQL_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions"
  val SPARK_EXECUTOR_CORES_THRESHOLD_KEY = "spark_executor_cores_threshold"
  val SPARK_EXECUTOR_MEMORY_THRESHOLD_KEY = "spark_executor_memory_threshold"
  val DEFAULT_SPARK_EXECUTOR_CORES_THRESHOLD : Int = 4
  val DEFAULT_SPARK_EXECUTOR_MEMORY_THRESHOLD : String  ="10GB"
  val SPARK_SQL_SHUFFLE_PARTITIONS_DEFAULT = 200
  val DEFAULT_SPARK_MAX_DATA_PROCESSED_THRESHOLD = "3GB"

  class Evaluator(executorStorageSpillHeuristic: ExecutorStorageSpillHeuristic, data: SparkApplicationData, stageAnalysis: Seq[StageAnalysis]) {
    lazy val executorAndDriverSummaries: Seq[ExecutorSummary] = data.executorSummaries
    if (executorAndDriverSummaries == null) {
      throw new Exception("Executors Summary is null.")
    }
    lazy val executorSummaries: Seq[ExecutorSummary] = executorAndDriverSummaries.filterNot(_.id.equals("driver"))
    if (executorSummaries.isEmpty) {
      throw new Exception("No executor information available.")
    }
    lazy val appConfigurationProperties: Map[String, String] =
      data.appConfigurationProperties
    val stageData: Seq[StageData] = data.stageDatas
    lazy val stagesWithSpill: Seq[Int] = stageData.filter(_.memoryBytesSpilled > 0).map(_.stageId)
    val partitionCount: Int = appConfigurationProperties.get(SPARK_SQL_SHUFFLE_PARTITIONS).map(_.toInt).getOrElse(SPARK_SQL_SHUFFLE_PARTITIONS_DEFAULT).asInstanceOf[Int]
    val executorCores: Int = appConfigurationProperties.get(SPARK_EXECUTOR_CORES).map(_.toInt).getOrElse(0).asInstanceOf[Number].intValue
    val totalTasks: Int = stageData.map(_.numTasks).sum
    lazy val totalMemorySpilled: Long = stageAnalysis.map(_.executionMemorySpillResult.memoryBytesSpilled).sum
    lazy val maxMemorySpilled: Long = stageAnalysis.map(_.executionMemorySpillResult.memoryBytesSpilled).max
    lazy val meanMemorySpilled: Long = totalMemorySpilled/totalTasks
    val taskSkewSeverity: Severity = stageAnalysis.map(_.taskSkewResult.severity).reduceLeft((a, b) => if (a.getValue > b.getValue) a else b)
    val maxAmountOfDataProcessed: Long = stageData.map(x => Seq(x.inputBytes, x.outputBytes, x.shuffleReadBytes, x.shuffleWriteBytes).max).max
    val medianTaskRunTime: Long = stageAnalysis.map(_.longTaskResult.medianRunTime.map(_.toLong).getOrElse(0L)).max
    lazy val inputBytes: Long = stageAnalysis.map(_.executionMemorySpillResult.inputBytes).max

    val severity: Severity = if (maxAmountOfDataProcessed < MemoryFormatUtils.stringToBytes(DEFAULT_SPARK_MAX_DATA_PROCESSED_THRESHOLD)) {
      stageAnalysis.map(_.executionMemorySpillResult.severity).reduceLeft((a, b) => if (a.getValue > b.getValue) a else b)
    } else {
      Severity.NONE
    }
    lazy val executorCount: Int = executorSummaries.size
    lazy val score: Int = severity.getValue * executorCount
    lazy val sparkExecutorMemory: Long = appConfigurationProperties.get(SPARK_EXECUTOR_MEMORY).map(MemoryFormatUtils.stringToBytes).getOrElse(0)
    lazy val sparkExecutorCores: Int = appConfigurationProperties.get(SPARK_EXECUTOR_CORES).map(_.toInt).getOrElse(0)
  }
}

