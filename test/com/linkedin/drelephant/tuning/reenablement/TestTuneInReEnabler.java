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

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import models.JobDefinition;
import models.JobSuggestedParamSet;
import models.TuningJobDefinition;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.Application;
import play.GlobalSettings;
import play.test.FakeApplication;

import static common.DBTestUtil.*;
import static common.TestConstants.*;
import static play.test.Helpers.*;


public class TestTuneInReEnabler {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private static FakeApplication fakeApp;

  private final static long testJobDefintionId_1 = 100149;
  private final static long testJobDefintionId_2 = 100150;
  private final static String ALL_HEURISTICS_PASSED = "All Heuristics Passed";

  private final double delta = 0.000001;

  private void populateTestData() {
    try {
      initTuneInReEnableMockDB();
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

  private TuningJobDefinition getTuningJobDefinition(long jobDefinitionId) {
    return TuningJobDefinition.find.select("*")
        .where()
        .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.id, jobDefinitionId)
        .findUnique();
  }

  @Test
  public void testGetTuneInDisabledJobs() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        TuneInReEnabler tuneInReEnabler = new TuneInReEnabler();
        Assert.assertEquals(3, tuneInReEnabler.getTuneInDisabledJobs().size());
      }
    });
  }

  @Test
  public void testReEnableWhenAutoApplyDisabled() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        TuningJobDefinition tjd = getTuningJobDefinition(testJobDefintionId_1);
        Assert.assertFalse(tjd.tuningEnabled);
        Assert.assertFalse(tjd.autoApply);
        Assert.assertNotNull(tjd.tuningDisabledReason);
        Assert.assertNotEquals(tjd.tuningDisabledReason, "");
        TuneInReEnabler tuneInReEnabler = new TuneInReEnabler();
        tuneInReEnabler.reEnableAutoTuning(tjd);
        Assert.assertFalse(tjd.tuningEnabled);
      }
    });
  }

  @Test
  public void testReEnableTuning() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        TuningJobDefinition tjd = getTuningJobDefinition(testJobDefintionId_2);
        Assert.assertFalse(tjd.tuningEnabled);
        Assert.assertTrue(tjd.autoApply);
        Assert.assertNotNull(tjd.tuningDisabledReason);
        Assert.assertEquals(ALL_HEURISTICS_PASSED, tjd.tuningDisabledReason);

        TuneInReEnabler tuneInReEnabler = new TuneInReEnabler();
        tuneInReEnabler.reEnableAutoTuning(tjd);
        Assert.assertTrue(tjd.tuningEnabled);
        Assert.assertNotNull(tjd.tuningDisabledReason);
        Assert.assertEquals(StringUtils.EMPTY, tjd.tuningDisabledReason);

        JobSuggestedParamSet bestJobSuggestedParamSet = JobSuggestedParamSet.find.select("*")
            .where()
            .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id, tjd.job.id)
            .eq(JobSuggestedParamSet.TABLE.isParamSetBest, true)
            .findUnique();
        Assert.assertNull(bestJobSuggestedParamSet);

        TuningJobDefinition updatedTuningJobDefinition = getTuningJobDefinition(testJobDefintionId_2);
        Assert.assertEquals(1, updatedTuningJobDefinition.tuningReEnablementCount.intValue());
        Assert.assertNotNull(updatedTuningJobDefinition.tuningReEnableTimestamp);
      }
    });
  }
}