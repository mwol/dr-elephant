#
# Copyright 2016 LinkedIn Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

# --- !Ups

CREATE TABLE IF NOT EXISTS tunein_re_enable_details (
    id int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT 'Id',
    job_definition_id int(10) unsigned NOT NULL COMMENT 'foreign key from job_definition table',
    tunein_re_enablement_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when tunein re-enabled for job',
    re_enablement_count int DEFAULT 1 COMMENT 'Number of times tunein is re-enabled till now',
    UNIQUE KEY tunein_re_enable_details_u1 (job_definition_id),
    CONSTRAINT tunein_re_enable_details_f1 FOREIGN KEY (job_definition_id) REFERENCES job_definition (id)
) ENGINE=InnoDB;

# --- !Downs
DROP TABLE tune_in_re_enable_details;