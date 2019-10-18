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

package com.linkedin.drelephant.tuning.reenablement;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.drelephant.AutoTuner;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.tuning.TuningHelper;
import com.linkedin.drelephant.util.Utils;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;
import models.JobSuggestedParamSet;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import static com.linkedin.drelephant.util.Utils.*;


public class TuneInReEnabler implements Runnable{

  private static final int DEFAULT_MIN_EXECUTIONS_BEFORE_TUNEIN_TO_RE_ENABLE = 14;
  private static final int DEFAULT_MAX_ITERATION_AFTER_TUNEIN_RE_ENABLE = 10;
  private static final int DEFAULT_MIN_TUNEIN_DISABLED_DURATION_IN_DAYS = 14;
  private static final long DEFAULT_TUNEIN_RE_ENABLE_DAEMON_WAIT_INTERVAL = AutoTuner.ONE_DAY;
  private static final String TUNEIN_RE_ENABLE_DAEMON_WAIT_INTERVAL =
      "tuning.reenable.wait.interval.ms";
  private static final String MIN_EXECUTIONS_BEFORE_TUNEIN_TO_RE_ENABLE =
      "tuning.min.executions.before.tunein.re_enable";
  private static final String MAX_ITERATIONS_AFTER_TUNEIN_RE_ENABLE =
      "tuning.max.iteration.after.tunein.re_enable";
  private static final String MIN_TUNEIN_DISABLED_DURATION_IN_DAYS_KEY =
      "tuning.min.tunein.disabled.duration.in_days";

  private long waitInterval;
  private int minDurationForTuneinToBeDisabled;
  private int minNumberOfExecutionsBeforeTuneInReEnable;
  private int numberOfIterationAfterTuneinReEnable;

  private static final Logger logger = Logger.getLogger(TuneInReEnabler.class);

  public TuneInReEnabler() {
    Configuration autoTuningConfiguration;
    autoTuningConfiguration = ElephantContext.instance().getAutoTuningConf();
    waitInterval = Utils.getNonNegativeLong(autoTuningConfiguration, TUNEIN_RE_ENABLE_DAEMON_WAIT_INTERVAL,
        DEFAULT_TUNEIN_RE_ENABLE_DAEMON_WAIT_INTERVAL);
    minDurationForTuneinToBeDisabled = autoTuningConfiguration.getInt(MIN_TUNEIN_DISABLED_DURATION_IN_DAYS_KEY,
            DEFAULT_MIN_TUNEIN_DISABLED_DURATION_IN_DAYS);
    minNumberOfExecutionsBeforeTuneInReEnable = autoTuningConfiguration.getInt(
        MIN_EXECUTIONS_BEFORE_TUNEIN_TO_RE_ENABLE, DEFAULT_MIN_EXECUTIONS_BEFORE_TUNEIN_TO_RE_ENABLE);
    numberOfIterationAfterTuneinReEnable = autoTuningConfiguration.getInt(MAX_ITERATIONS_AFTER_TUNEIN_RE_ENABLE,
        DEFAULT_MAX_ITERATION_AFTER_TUNEIN_RE_ENABLE);
  }

  public void run() {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        logger.info("Starting process to Re-Enable TuneIn");
        List<TuningJobDefinition> tuneinDisabledJobList = getTuneInDisabledJobs();
        if (tuneinDisabledJobList != null) {
          logger.info("#Jobs for with TuneIn Disabled " + tuneinDisabledJobList.size());
          //Batch size so that we don't enable tuning for all the disabled jobs in one go
          int batchSize = (tuneinDisabledJobList.size() / minDurationForTuneinToBeDisabled);
          if (batchSize == 0) {
            batchSize = tuneinDisabledJobList.size();
          }
          tuneinDisabledJobList.stream().limit(batchSize).collect(Collectors.toList()).forEach(this::reEnableAutoTuning);
        }
        logger.info("Exiting the thread responsible for Re-Enabling TuneIn");
        Thread.sleep(waitInterval);
      } catch (InterruptedException ex) {
        logger.info("TuneIn ReEnablement thread interrupted", ex);
      }
    }
  }

  @VisibleForTesting
  protected List<TuningJobDefinition> getTuneInDisabledJobs() {
    return TuningJobDefinition.find.select("*")
        .where()
        .eq(TuningJobDefinition.TABLE.tuningEnabled, false)
        .findList();
  }

  @VisibleForTesting
  void reEnableAutoTuning(TuningJobDefinition tuningJobDefinition) {
    logger.info("Analyzing job definition id for TuneIn re-enable " + tuningJobDefinition.job.id);
    if ( tuningJobDefinition.tuningEnabled || !tuningJobDefinition.autoApply ||
        tuningJobDefinition.tuningDisabledReason == null ) {
      return;
    }
    if (isJobEligibleForTuneInReEnablement(tuningJobDefinition)) {
      logger.info(String.format("%s is eligible for tuning re-enablement", tuningJobDefinition.job.id));
      processReEnablementForTuneIn(tuningJobDefinition);
      return;
    }
    logger.info("No of valid execution till now " + TuningHelper.getNumberOfValidSuggestedParamExecution(
        TuningHelper.getTuningJobExecutionFromDefinition(tuningJobDefinition.job)
    ));
    logger.info(String.format("%s is not eligible for TuneIn re-enablement", tuningJobDefinition.job.id));
  }

  private boolean isJobEligibleForTuneInReEnablement(TuningJobDefinition tuningJobDefinition) {
    long maxAllowedTuneinDisableDurationInMs =
        getTimeInMilliSecondsFromDays(minDurationForTuneinToBeDisabled);

    boolean isJobEligibleForTuneinReEnable = isTuneInDisabledForSpecifiedIterations(
        tuningJobDefinition, minNumberOfExecutionsBeforeTuneInReEnable) && isTuneInDisabledForSpecifiedDuration(
            tuningJobDefinition, maxAllowedTuneinDisableDurationInMs);
    return isJobEligibleForTuneinReEnable;
  }

  private boolean isTuneInDisabledForSpecifiedDuration(TuningJobDefinition
      tuningJobDefinition, long disabledDurationInMs) {
    if (logger.isDebugEnabled()) {
      logger.debug(String.format("Checking if TuneIn for job %s is disabled for more than duration in ms %s}",
          tuningJobDefinition.job.id, disabledDurationInMs));
    }
    JobSuggestedParamSet latestSuggestedJobParamSet =
        TuningHelper.getLatestSuggestedJobParamSet(tuningJobDefinition.job.id);
    long tuneinDisabledEpochTime = latestSuggestedJobParamSet.updatedTs
        .getTime();
    long currentEpochTime = System.currentTimeMillis();
    return (currentEpochTime - tuneinDisabledEpochTime) >= disabledDurationInMs;
  }

  private boolean isTuneInDisabledForSpecifiedIterations(TuningJobDefinition
      tuningJobDefinition, int minExecutionCount) {
    if (logger.isDebugEnabled()) {
      logger.debug(String.format("Checking if job %s had min %s executions with TuneIn disabled",
          tuningJobDefinition.job.id, minExecutionCount));
    }
    List<TuningJobExecutionParamSet> lastJobExecutionParamSets = TuningHelper.getLastNExecutionParamSets(
        tuningJobDefinition.job.id, minExecutionCount);
    if (lastJobExecutionParamSets == null || lastJobExecutionParamSets.size() != minExecutionCount) {
      return false;
    }
    TuningJobExecutionParamSet executionWithTuningEnabled =
        lastJobExecutionParamSets.stream().filter(el -> el.tuningEnabled).findFirst().orElse(null);
    return (executionWithTuningEnabled == null);
  }

  private void processReEnablementForTuneIn(TuningJobDefinition tuningJobDefinition) {
    logger.info("Initiating the process for re-enabling Auto-Tuning");
    updateIterationCountForJob(tuningJobDefinition,
        numberOfIterationAfterTuneinReEnable);
    tuningJobDefinition.tuningDisabledReason = StringUtils.EMPTY;
    updateJobSuggestedParamSetForReEnable(tuningJobDefinition);
    tuningJobDefinition.tuningEnabled = true;
    tuningJobDefinition.tuningReEnableTimestamp= new Timestamp(System.currentTimeMillis());
    if (tuningJobDefinition.tuningReEnablementCount == null) {
      tuningJobDefinition.tuningReEnablementCount = 1;
    } else {
      tuningJobDefinition.tuningReEnablementCount++;
    }
    tuningJobDefinition.update();
  }

  private void updateIterationCountForJob(TuningJobDefinition
      tuningJobDefinition, int maxTuneinIterationAfterReEnable) {
    int iterationCountWithSuggestedParamSet = TuningHelper
        .getNumberOfValidSuggestedParamExecution( TuningHelper
            .getTuningJobExecutionFromDefinition(tuningJobDefinition.job));
    tuningJobDefinition.numberOfIterations = Math.max( tuningJobDefinition.
        numberOfIterations, (iterationCountWithSuggestedParamSet +
        maxTuneinIterationAfterReEnable));
    logger.info(String.format("Updating the iterationCount to %s for jobDefintionId %s",
        tuningJobDefinition.numberOfIterations, tuningJobDefinition.job.id));
  }

  private void updateJobSuggestedParamSetForReEnable(TuningJobDefinition tuningJobDefinition) {
    unMarkCurrentBestParamSet(tuningJobDefinition.job.id);
  }

  private void unMarkCurrentBestParamSet(Integer jobDefinitionId) {
    JobSuggestedParamSet bestJobSuggestedParamSet = TuningHelper.getBestParamSet(jobDefinitionId);
    if (bestJobSuggestedParamSet != null) {
      logger.info(String.format("For Re-Enable de-marking jobSuggestedParamSet id %s as best",
          bestJobSuggestedParamSet.id));
      bestJobSuggestedParamSet.isParamSetBest = false;
      bestJobSuggestedParamSet.update();
    }
  }
}