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

package com.linkedin.drelephant.tuning;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.JobDefinition;
import models.JobSuggestedParamSet;
import models.TuningJobExecutionParamSet;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.Application;
import play.GlobalSettings;
import play.test.FakeApplication;

import static common.DBTestUtil.*;
import static common.TestConstants.*;
import static org.junit.Assert.*;
import static play.test.Helpers.*;


public class TestTuningHelper {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private static FakeApplication fakeApp;

  private final static int testJobDefintionId_1 = 1;
  private final static int testJobDefintionId_2 = 2;

  private void populateTestData() {
    try {
      initTuningHelperTestMockDB();
    } catch (IOException ioEx) {
      logger.error("IOException encountered while populating test data", ioEx);
    } catch (SQLException sqlEx) {
      logger.error("SqlException encountered while populating test data", sqlEx);
    }
  }

  @Before
  public void setup() {
    Map<String, String> dbConn = new HashMap<>();
    dbConn.put(DB_DEFAULT_DRIVER_KEY, DB_DEFAULT_DRIVER_VALUE);
    dbConn.put(DB_DEFAULT_URL_KEY, DB_DEFAULT_URL_VALUE);
    dbConn.put(EVOLUTION_PLUGIN_KEY, EVOLUTION_PLUGIN_VALUE);
    dbConn.put(APPLY_EVOLUTIONS_DEFAULT_KEY, APPLY_EVOLUTIONS_DEFAULT_VALUE);

    GlobalSettings gs = new GlobalSettings() {
      @Override
      public void onStart(Application app) {
        logger.info("Starting FakeApplication");
      }
    };

    fakeApp = fakeApplication(dbConn, gs);
  }

  @Test
  public void testValidSuggestedExecutionCountAfterTuneInReEnable() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        JobDefinition jobDefinition_1 = JobDefinition.find.byId(testJobDefintionId_1);
        JobDefinition jobDefinition_2 = JobDefinition.find.where().eq(JobDefinition.TABLE.id,
            testJobDefintionId_2)
            .findUnique();
        List<TuningJobExecutionParamSet> tuningJobExecutionParamSets_1 = TuningHelper
            .getTuningJobExecutionFromDefinition(jobDefinition_1);
        List<TuningJobExecutionParamSet> tuningJobExecutionParamSets_2 = TuningHelper
            .getTuningJobExecutionFromDefinition(jobDefinition_2);
        int validSuggestedExecutionCountAfterTuneinReEnable_1 = TuningHelper.
            getValidSuggestedParamExecutionCountAfterTuneInReEnable(testJobDefintionId_1,
                tuningJobExecutionParamSets_1);
        int validSuggestedExecutionCountAfterTuneinReEnable_2 = TuningHelper.
            getValidSuggestedParamExecutionCountAfterTuneInReEnable(testJobDefintionId_2,
                tuningJobExecutionParamSets_2);

        assertEquals(9, validSuggestedExecutionCountAfterTuneinReEnable_2);
        assertEquals(Integer.MAX_VALUE, validSuggestedExecutionCountAfterTuneinReEnable_1);
      }
    });
  }

  @Test
  public void testGetTuneinEnableTimestamp() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        assertNull(TuningHelper.getTuneInReEnablementTimestamp(testJobDefintionId_1));
        assertEquals(Timestamp.valueOf("2019-08-29 08:20:50"), TuningHelper.getTuneInReEnablementTimestamp(
            testJobDefintionId_2));
      }
    });
  }

  @Test
  public void testGetLatestSuggestedJobParamSet() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        JobSuggestedParamSet jobSuggestedParamSet_1 = TuningHelper.getLatestSuggestedJobParamSet(testJobDefintionId_1);
        JobSuggestedParamSet jobSuggestedParamSet_2 = TuningHelper.getLatestSuggestedJobParamSet(testJobDefintionId_2);
        assertEquals(2071, jobSuggestedParamSet_1.id.intValue());
        assertEquals(testJobDefintionId_1, jobSuggestedParamSet_1.jobDefinition.id.longValue());
        assertEquals(2074, jobSuggestedParamSet_2.id.intValue());
        assertEquals(testJobDefintionId_2, jobSuggestedParamSet_2.jobDefinition.id.longValue());
      }
    });
  }

  @Test
  public void testGetLastNExecutionParamSets() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      @Override
      public void run() {
        populateTestData();
        int executionCount = 5;
        List<TuningJobExecutionParamSet> lastNExecutionParamSets_1  = TuningHelper.getLastNExecutionParamSets(
            testJobDefintionId_1, executionCount);
        List<TuningJobExecutionParamSet> lastNExecutionParamSets_2 = TuningHelper.getLastNExecutionParamSets(
            testJobDefintionId_2, executionCount);
        assertEquals(1, lastNExecutionParamSets_1.size());
        assertEquals(5, lastNExecutionParamSets_2.size());
        assertEquals(3318, lastNExecutionParamSets_2.get(4).jobExecution.id.intValue());
        assertEquals(3330, lastNExecutionParamSets_2.get(0).jobExecution.id.intValue());
      }
    });
  }

  @Test
  public void testGetDefaultParamSet() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      @Override
      public void run() {
        populateTestData();
        JobSuggestedParamSet defaultJobSuggestedParamSet_1, defaultJobSuggestedParamSet_2;
        defaultJobSuggestedParamSet_1 = TuningHelper.getDefaultParamSet(testJobDefintionId_1);
        defaultJobSuggestedParamSet_2 = TuningHelper.getDefaultParamSet(testJobDefintionId_2);
        assertEquals(1678, defaultJobSuggestedParamSet_1.id.intValue());
        assertEquals(testJobDefintionId_1, defaultJobSuggestedParamSet_1.jobDefinition.id.intValue());
        assertEquals(false, defaultJobSuggestedParamSet_1.isParamSetBest);
        assertEquals(false, defaultJobSuggestedParamSet_1.isParamSetSuggested);
        assertEquals(true, defaultJobSuggestedParamSet_1.isParamSetDefault);
        assertNull(defaultJobSuggestedParamSet_2);
      }
    });
  }

  @Test
  public void testGetBestParamSet() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      @Override
      public void run() {
        populateTestData();
        JobSuggestedParamSet bestJobSuggestedParamSet_1, bestJobSuggestedParamSet_2;
        bestJobSuggestedParamSet_1 = TuningHelper.getBestParamSet(testJobDefintionId_1);
        bestJobSuggestedParamSet_2 = TuningHelper.getBestParamSet(testJobDefintionId_2);
        assertEquals(2071, bestJobSuggestedParamSet_1.id.intValue());
        assertEquals(testJobDefintionId_1, bestJobSuggestedParamSet_1.jobDefinition.id.intValue());
        assertEquals(true, bestJobSuggestedParamSet_1.isParamSetBest);
        assertEquals(true, bestJobSuggestedParamSet_1.isParamSetSuggested);
        assertEquals(false, bestJobSuggestedParamSet_1.isParamSetDefault);
        assertEquals(2074, bestJobSuggestedParamSet_2.id.intValue());
        assertEquals(testJobDefintionId_2, bestJobSuggestedParamSet_2.jobDefinition.id.intValue());
        assertEquals(true, bestJobSuggestedParamSet_2.isParamSetBest);
        assertEquals(true, bestJobSuggestedParamSet_2.isParamSetSuggested);
        assertEquals(false, bestJobSuggestedParamSet_2.isParamSetDefault);
      }
    });
  }
}