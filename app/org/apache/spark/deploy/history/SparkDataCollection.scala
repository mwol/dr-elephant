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

package org.apache.spark.deploy.history

import java.io.InputStream
import java.util.{ArrayList => JArrayList, HashSet => JHashSet, List => JList, Set => JSet}

import com.linkedin.drelephant.analysis.ApplicationType
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.spark.fetchers.statusapiv1._
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.ReplayListenerBus
import org.apache.spark.status.api.v1
import org.apache.spark.status.config.ASYNC_TRACKING_ENABLED
import org.apache.spark.status.{AppStatusListener, AppStatusStore, ElementTrackingStore}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.util.kvstore.{InMemoryStore, KVStore}

import scala.collection.mutable


/**
  * This class wraps the logic of collecting the data in SparkEventListeners into the
  * HadoopApplicationData instances.
  *
  * Notice:
  * This has to live in Spark's scope because ApplicationEventListener is in private[spark] scope. And it is problematic
  * to compile if written in Java.
  */
class SparkDataCollection {
  import SparkDataCollection._

  private val _conf = new SparkConf
  private var _isThrottled: Boolean = false;

  var environmentInfo: v1.ApplicationEnvironmentInfo = null
  var executorSummary: Seq[v1.ExecutorSummary] = null
  var jobData: Seq[v1.JobData] = null
  var stageData: Seq[v1.StageData] = null
  var appInfo: v1.ApplicationInfo = null

  def throttle(): Unit = {
    _isThrottled = true
  }

  def isThrottled: Boolean = _isThrottled

  def getApplicationType: ApplicationType = APPLICATION_TYPE

  def isEmpty: Boolean = !isThrottled

  def getApplicationInfo: v1.ApplicationInfo = {
    appInfo
  }

  def getExecutorSummary: Seq[v1.ExecutorSummary] = {
    executorSummary
  }

  def getJobData: Seq[v1.JobData] = {
    jobData
  }

  def getStageData: Seq[v1.StageData] = {
    stageData
  }

  def getAppEnvironment: Map[String, String] = {
    environmentInfo.sparkProperties.toMap
  }

  def replayEventLogs(in: InputStream, sourceName: String): Unit = {
    val store: KVStore = createInMemoryStore()
    val replayConf: SparkConf = _conf.clone().set(ASYNC_TRACKING_ENABLED, false)
    val trackingStore: ElementTrackingStore = new ElementTrackingStore(store, replayConf)
    val replayBus: ReplayListenerBus = new ReplayListenerBus()
    val listener: AppStatusListener = new AppStatusListener(trackingStore, replayConf, false)
    replayBus.addListener(listener)

    try {
      replayBus.replay(in, sourceName, true)
      trackingStore.close(false)
    } catch {
      case e: Exception =>
        Utils.tryLogNonFatalError {
          trackingStore.close()
        }
        throw e
    }
    val appStatusStore: AppStatusStore = new AppStatusStore(store)
    appInfo = appStatusStore.applicationInfo()
    environmentInfo = appStatusStore.environmentInfo()
    executorSummary = appStatusStore.executorList(true)
    jobData = appStatusStore.jobsList(null)
    stageData = appStatusStore.stageList(null)
    appStatusStore.close()
  }

  def getSparkApplicationData: SparkApplicationData = {
    new SparkApplicationData(
      appInfo.id,
      getAppEnvironment,
      new ApplicationInfoImpl(appInfo.id, appInfo.name, appInfo.attempts.map(attempt =>
        new ApplicationAttemptInfoImpl(
          attempt.attemptId,
          attempt.startTime,
          attempt.endTime,
          attempt.sparkUser,
          attempt.completed
        ))),
      extractJobData(jobData),
      extractStageData(stageData),
      extractExecutorSummary(executorSummary)
    )
  }

  private def extractJobData(datas: Seq[v1.JobData]): Seq[JobData] = {
    datas.map(jobInfo =>
      new JobDataImpl(
        jobInfo.jobId,
        jobInfo.jobId.toString,
        jobInfo.description,
        jobInfo.submissionTime,
        jobInfo.completionTime,
        jobInfo.stageIds,
        jobInfo.jobGroup,
        jobInfo.status,
        jobInfo.numTasks,
        jobInfo.numActiveTasks,
        jobInfo.numCompletedTasks,
        jobInfo.numSkippedTasks,
        jobInfo.numFailedTasks,
        jobInfo.numActiveStages,
        jobInfo.numCompletedStages,
        jobInfo.numSkippedStages,
        jobInfo.numFailedStages
      )
    )
  }

  private def extractStageData(stageData: Seq[v1.StageData]): Seq[StageDataImpl] = {
    stageData.map(stageInfo =>
      new StageDataImpl(
        stageInfo.status,
        stageInfo.stageId,
        stageInfo.attemptId,
        stageInfo.numActiveTasks,
        stageInfo.numCompleteTasks,
        stageInfo.numFailedTasks,
        stageInfo.executorRunTime,
        stageInfo.inputBytes,
        inputRecords = 0,
        stageInfo.outputBytes,
        outputRecords = 0,
        stageInfo.shuffleReadBytes,
        shuffleReadRecords = 0,
        stageInfo.shuffleWriteBytes,
        shuffleWriteRecords = 0,
        stageInfo.memoryBytesSpilled,
        stageInfo.diskBytesSpilled,
        stageInfo.name,
        stageInfo.details,
        stageInfo.schedulingPool,
        accumulatorUpdates = Seq.empty,
        tasks = None,
        executorSummary = None
      ))
  }

  private def extractExecutorSummary(executorSummarySeq: Seq[v1.ExecutorSummary]):
  Seq[ExecutorSummaryImpl] = {
    executorSummarySeq.map(executorSummary =>
      new ExecutorSummaryImpl(
        executorSummary.id,
        executorSummary.hostPort,
        executorSummary.rddBlocks,
        executorSummary.memoryUsed,
        executorSummary.diskUsed,
        executorSummary.activeTasks,
        executorSummary.failedTasks,
        executorSummary.completedTasks,
        executorSummary.totalTasks,
        executorSummary.totalDuration,
        executorSummary.totalInputBytes,
        executorSummary.totalShuffleRead,
        executorSummary.totalShuffleWrite,
        executorSummary.maxMemory,
        executorSummary.totalGCTime,
        executorLogs = Map.empty
      )
    )
  }


  private def createInMemoryStore(): KVStore = {
    val store = new InMemoryStore()
    store
  }
}

object SparkDataCollection {
  private val APPLICATION_TYPE = {
    new ApplicationType("SPARK")
  }

  def stringToSet(str: String): JSet[String] = {
    val set = new JHashSet[String]()
    str.split(",").foreach { case t: String => set.add(t) }
    set
  }

  def toJList[T](seq: Seq[T]): JList[T] = {
    val list = new JArrayList[T]()
    seq.foreach { case (item: T) => list.add(item) }
    list
  }

  def addIntSetToJSet(set: OpenHashSet[Int], jset: JSet[Integer]): Unit = {
    val it = set.iterator
    while (it.hasNext) {
      jset.add(it.next())
    }
  }

  def addIntSetToJSet(set: mutable.HashSet[Int], jset: JSet[Integer]): Unit = {
    val it = set.iterator
    while (it.hasNext) {
      jset.add(it.next())
    }
  }
}
