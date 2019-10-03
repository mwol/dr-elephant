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

package models;

import com.avaje.ebean.annotation.ConcurrencyMode;
import com.avaje.ebean.annotation.EntityConcurrencyMode;
import java.sql.Timestamp;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.Transient;
import com.avaje.ebean.annotation.UpdatedTimestamp;
import play.db.ebean.Model;


/**
 * This table have information about the details of tunein re-enablement of the job.
 */
@Entity
@EntityConcurrencyMode(ConcurrencyMode.NONE)
@Table(name = "tunein_re_enable_details")
public class TuneInReEnableDetails extends Model {

  public static class TABLE {
    public static final String TABLE_NAME = "tunein_re_enable_details";
    public static final String id = "id";
    public static final String jobDefinition = "jobDefinition";
    public static final String tuneinReEnablementTimestamp = "tunein_re_enablement_timestamp";
    public static final String reEnablementCount = "reEnablementCount";
  }

  @Id
  public int id;

  @Column(nullable = false)
  @OneToOne(cascade = CascadeType.ALL)
  @JoinTable(name = "job_definition", joinColumns = {@JoinColumn(name = "job_definition_id", referencedColumnName = "id")})
  public JobDefinition jobDefinition;

  @Column(nullable = false)
  public Timestamp tuneinReEnablementTimestamp;

  @Column(nullable = false)
  public Integer reEnablementCount;

  @Override
  public void save() {
    this.tuneinReEnablementTimestamp = new Timestamp(System.currentTimeMillis());
    super.save();
  }

  @Override
  public void update() {
    this.tuneinReEnablementTimestamp = new Timestamp(System.currentTimeMillis());
    super.update();
  }

  public static Finder<Integer, TuneInReEnableDetails> find = new Finder<Integer, TuneInReEnableDetails>(Integer.class, TuneInReEnableDetails.class);
}
