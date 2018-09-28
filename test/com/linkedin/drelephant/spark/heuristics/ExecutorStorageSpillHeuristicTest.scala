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

import scala.collection.JavaConverters
import com.linkedin.drelephant.analysis.{ApplicationType, Severity, SeverityThresholds}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.{SparkApplicationData, SparkLogDerivedData, SparkRestDerivedData}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationInfoImpl, ExecutorSummaryImpl, StageDataImpl, StageStatus}
import com.linkedin.drelephant.spark.heuristics.ExecutorStorageSpillHeuristic.Evaluator
import com.linkedin.drelephant.spark.heuristics.SparkTestUtilities.{StageBuilder, sdf}
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.scalatest.{FunSpec, Matchers}

/**
  * Test class for Executor Storage Spill Heuristic. It checks whether all the values used in the heuristic are calculated correctly.
  */
class ExecutorStorageSpillHeuristicTest extends FunSpec with Matchers {
  import ExecutorStorageSpillHeuristicTest._

  describe("ExecutorStorageSpillHeuristic") {
    val heuristicConfigurationData = newFakeHeuristicConfigurationData(
      Map.empty
    )
    val executorStorageSpillHeuristic = new ExecutorStorageSpillHeuristic(heuristicConfigurationData)

    val appConfigurationProperties = Map("spark.executor.memory" -> "4g", "spark.executor.cores"->"4", "spark.executor.instances"->"4")
    val appConfigurationProperties2 = Map("spark.executor.memory" -> "5g", "spark.executor.cores"->"4", "spark.executor.instances"->"4", "spark.sql.shuffle.partitions"->"4300")

    val executorSummaries = Seq(
      newFakeExecutorSummary(
        id = "1",
        totalMemoryBytesSpilled = 200000L,
        0
      ),
      newFakeExecutorSummary(
        id = "2",
        totalMemoryBytesSpilled = 100000L,
        0
      ),
      newFakeExecutorSummary(
        id = "3",
        totalMemoryBytesSpilled = 300000L,
        0
      ),
      newFakeExecutorSummary(
        id = "4",
        totalMemoryBytesSpilled = 200000L,
        1
      )
    )
    //total task count > 4000
    val executorSummaries2 = Seq(
      newFakeExecutorSummary(
        id = "1",
        totalMemoryBytesSpilled = 200000L,
        2500
      ),
      newFakeExecutorSummary(
        id = "2",
        totalMemoryBytesSpilled = 500000L,
        2000
      )
    )

    val stageData1: Seq[StageDataImpl] = Seq(
      StageBuilder(0,10).taskRuntime(100, 220, 300).spill(130, 250, 340).create(),
      StageBuilder(1,100).taskRuntime(50,250,500).shuffleRead(200, 300, 800)
        .spill(100, 150, 400).create()
    )

    val stageData2: Seq[StageDataImpl] = Seq(
      StageBuilder(0,10).taskRuntime(100, 220, 300).create(),
      StageBuilder(1,100).taskRuntime(50,250,500).shuffleRead(200, 300, 800).create()
    )


    describe(".apply") {
      val data1 = newFakeSparkApplicationData(executorSummaries, stageData1, appConfigurationProperties)
      val data2 = newFakeSparkApplicationData(executorSummaries2, stageData2, appConfigurationProperties2)
      val heuristicResult = executorStorageSpillHeuristic.apply(data1)
      val heuristicResult2 = executorStorageSpillHeuristic.apply(data2)
      val heuristicResultDetails = heuristicResult.getHeuristicResultDetails
      val heuristicResultDetails2 = heuristicResult2.getHeuristicResultDetails
      val stageAnalysisResult = new StagesAnalyzer(heuristicConfigurationData, data1).getStageAnalysis()
      val evaluator = new Evaluator(executorStorageSpillHeuristic, data1, stageAnalysisResult)

      it("returns the severity") {
        heuristicResult.getSeverity should be(Severity.CRITICAL)
      }

      it("return the data skew note") {
        heuristicResultDetails.get(6).getName should be("Data Skew")
      }

      it("return the data skew severity") {
        evaluator.taskSkewSeverity should be(Severity.MODERATE)
      }

      it("returns the score") {
        heuristicResult.getScore should be(evaluator.severity.getValue * data1.executorSummaries.size)
      }

      it("returns the total memory spilled") {
        val details = heuristicResultDetails.get(0)
        details.getName should include("Total memory spilled")
        details.getValue should be("740 MB")
      }

      it("returns the max memory spilled") {
        val details = heuristicResultDetails.get(1)
        details.getName should include("Max memory spilled")
        details.getValue should be("400 MB")
      }

      it("returns the mean memory spilled") {
        val details = heuristicResultDetails.get(2)
        details.getName should include("Mean memory spilled")
        details.getValue should be("6.73 MB")
      }

      it("returns no data skew due to more tasks and executor memory than the threshold") {
        heuristicResult2.getHeuristicResultDetails.size() should be(4)
      }
    }
  }
}

object ExecutorStorageSpillHeuristicTest {
  import JavaConverters._

  def newFakeHeuristicConfigurationData(params: Map[String, String] = Map.empty): HeuristicConfigurationData =
    new HeuristicConfigurationData("heuristic", "class", "view", new ApplicationType("type"), params.asJava)

  def newFakeExecutorSummary(
    id: String,
    totalMemoryBytesSpilled: Long,
    totalTasks: Int
  ): ExecutorSummaryImpl = new ExecutorSummaryImpl(
    id,
    hostPort = "",
    rddBlocks = 0,
    memoryUsed=0,
    diskUsed = 0,
    activeTasks = 0,
    failedTasks = 0,
    completedTasks = 0,
    totalTasks,
    maxTasks = 10,
    totalDuration=0,
    totalInputBytes=0,
    totalShuffleRead=0,
    totalShuffleWrite= 0,
    maxMemory= 2000,
    totalGCTime = 0,
    totalMemoryBytesSpilled,
    executorLogs = Map.empty,
    peakJvmUsedMemory = Map.empty,
    peakUnifiedMemory = Map.empty
  )

  def newFakeSparkApplicationData(
    executorSummaries: Seq[ExecutorSummaryImpl],
    stageData: Seq[StageDataImpl],
    appConfigurationProperties: Map[String, String]
  ): SparkApplicationData = {
    val appId = "application_1"

    val restDerivedData = SparkRestDerivedData(
      new ApplicationInfoImpl(appId, name = "app", Seq.empty),
      jobDatas = Seq.empty,
      stageDatas = stageData,
      executorSummaries = executorSummaries,
      stagesWithFailedTasks = Seq.empty
    )

    val logDerivedData = SparkLogDerivedData(
      SparkListenerEnvironmentUpdate(Map("Spark Properties" -> appConfigurationProperties.toSeq))
    )

    SparkApplicationData(appId, restDerivedData, Some(logDerivedData))
  }
}
