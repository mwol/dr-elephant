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

package com.linkedin.drelephant.exceptions.azkaban;

import com.linkedin.drelephant.clients.WorkflowClient;
import com.linkedin.drelephant.configurations.scheduler.SchedulerConfigurationData;
import com.linkedin.drelephant.exceptions.util.ExceptionInfo;
import com.linkedin.drelephant.util.InfoExtractor;
import com.linkedin.drelephant.util.Utils;
import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;

import static com.linkedin.drelephant.exceptions.util.ExceptionUtils.ConfigurationBuilder.*;
import static com.linkedin.drelephant.exceptions.util.ExceptionUtils.*;
import static controllers.api.v1.JsonKeys.*;


/**
 * This class servers the purpose of analyzing Azkaban Logs for a job
 * and filter out exception stacktraces based on some pre-defined exception patterns
 */
public class AzkabanExceptionLogAnalyzer {

  private final String flowExecUrl;
  private final String jobExecUrl;
  private WorkflowClient _workflowClient;
  private List<ExceptionInfo> azkabanExceptionInfoList = new ArrayList<>();
  private final String SCHEDULER_NAME = "azkaban";
  private final String PRIVATE_KEY = "private_key";

  private HashSet<String> exceptionIdSet = new HashSet<>();

  private static final Logger logger = Logger.getLogger(AzkabanExceptionLogAnalyzer.class);

  //For controlling the way instance is created
  private AzkabanExceptionLogAnalyzer() {
    flowExecUrl = null;
    jobExecUrl = null;
  }

  public AzkabanExceptionLogAnalyzer(String flowExecUrl, String jobExecUrl) {
    this.flowExecUrl = flowExecUrl;
    this.jobExecUrl = jobExecUrl;
  }

  private void init() {
    String jobId = getJobName(jobExecUrl);
    _workflowClient = getWorkflowClient(flowExecUrl);
    logger.info("Fetching job logs from Azkaban for " + jobExecUrl);
    String jobLog = _workflowClient.getAzkabanJobLog(jobId, AZKABAN_JOB_LOG_START_OFFSET.getValue()
        .toString(), AZKABAN_JOB_LOG_MAX_LENGTH.getValue().toString());
    analyzeJobLog(processJobLog(jobLog));
  }

  /**
   * @return Exceptions Info list containing logs relevant to failure cause
   */
  public List<ExceptionInfo> getExceptionInfoList() {
    if (azkabanExceptionInfoList.isEmpty()) {
      init();
    }
    return azkabanExceptionInfoList;
  }

  /**
   * @param jobLogs Log for the job provided by Azkaban
   * @return Filtered out exceptions relevant to failure cause
   */
  private void analyzeJobLog(String jobLogs) {
    logger.info("Analyzing job logs from Azkaban of length " + jobLogs.length());
    List<Pattern> exceptionRegexList = new ArrayList<>();
    for (String exceptionRegex : REGEX_FOR_EXCEPTION_PATTERN_IN_AZKABAN_LOGS.getValue()) {
      exceptionRegexList.add(Pattern.compile(exceptionRegex));
    }
    for (Pattern exceptionPattern : exceptionRegexList) {
      Matcher exceptionPatternMatcher = exceptionPattern.matcher(jobLogs);
      while (exceptionPatternMatcher.find()) {
        if (!isExceptionLogDuplicate(exceptionPatternMatcher.group(0))) {
          ExceptionInfo exceptionInfo = new ExceptionInfo();
          exceptionInfo.setExceptionID(exceptionPatternMatcher.group(0).hashCode());
          exceptionInfo.setExceptionName(exceptionPatternMatcher.group(1));
          exceptionInfo.setExceptionSource(ExceptionInfo.ExceptionSource.SCHEDULER);
          exceptionInfo.setWeightOfException(exceptionIdSet.size());
          exceptionInfo.setExceptionStackTrace(Utils.truncateStackTrace(exceptionPatternMatcher.group(0),
              MAX_LINE_LENGTH_OF_EXCEPTION.getValue()));
          azkabanExceptionInfoList.add(exceptionInfo);
        }
      }
    }
  }

  /**
   * @param flowExecUrl Azkaban flowExection Url for creating the workflow client
   * @return Workflow client for connecting to Azkaban and getting flow specific information
   */
  protected WorkflowClient getWorkflowClient(String flowExecUrl) {
    WorkflowClient workflowClient = InfoExtractor.getWorkflowClientInstance(SCHEDULER_NAME, flowExecUrl);
    SchedulerConfigurationData schedulerData = InfoExtractor.getSchedulerData(SCHEDULER_NAME);
    if (schedulerData == null) {
      throw new RuntimeException(String.format("No scheduler data found for scheduler %s", SCHEDULER_NAME));
    }

    if (!schedulerData.getParamMap().containsKey(USERNAME)) {
      throw new RuntimeException("Username Not Found for login");
    }

    String username = schedulerData.getParamMap().get(USERNAME);
    if (schedulerData.getParamMap().containsKey(PRIVATE_KEY)) {
      workflowClient.login(username, new File(schedulerData.getParamMap().get(PRIVATE_KEY)));
    } else if (schedulerData.getParamMap().containsKey(PASSWORD)) {
      workflowClient.login(username, schedulerData.getParamMap().get(PASSWORD));
    } else {
      throw new RuntimeException("Neither private key nor password was specified for scheduler " + SCHEDULER_NAME);
    }
    return workflowClient;
  }

  /**
   * @param exceptionStackTrace HashCode for filterOut duplicate exception stackTrace
   * @return whether this exception stackTrace is parsed and saved before
   */
  private boolean isExceptionLogDuplicate(String exceptionStackTrace) {
    if (exceptionIdSet.contains(exceptionStackTrace)) {
      for (String uniqueExceptions : exceptionIdSet) {
        if (uniqueExceptions.equals(exceptionStackTrace)) {
          return true;
        }
      }
    }
    exceptionIdSet.add(exceptionStackTrace);
    return false;
  }

  /**
   * @param logs Azkaban logs obtained from workflowClient
   * @return Logs after removing all the redundant log for exception fingerprinting
   */
  private String processJobLog(String logs) {
    for (String logPattern : REGEX_FOR_REDUNDANT_LOG_PATTERN_IN_AZKABAN_LOGS.getValue()) {
      logs = logs.replaceAll(logPattern, "");
    }
    return logs;
  }
}
