/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.samples.daily_precipitation_sample;

import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.Validation;

/**
 * Extra options for {@link PrecipitationPipeline}.
 *
 * @author jsvangeffen
 */
public interface PrecipitationOptions extends PipelineOptions {


  @Description("Set to true when appending to existing table, "
               + "false when overriding existing table")
  @Default.Boolean(true)
  @Validation.Required
  boolean getAppend();
  void setAppend(boolean value);

  @Description("Existing Google Cloud project to work with.")
  @Default.String("bqpipelines")
  @Validation.Required
  String getProject();
  void setProject(String value);

  @Description("GCS bucket where precipitation data files are stored.")
  @Default.String("jsv-test")
  @Validation.Required
  String getBucket();
  void setBucket(String value);

  @Description("Fully-qualified BigQuery table to update. "
               + "Should be in the format \"project:dataset.table\".")
  @Default.String("bqpipelines:weather.us_precipitation")
  @Validation.Required
  String getTable();
  void setTable(String value);

  @Description("First day of precipitation data to upload. "
               + "Should be in the format \"YYYYMMD\".\n"
               + "If left blank, all data up to the end date will be included")
  @Default.String("")
  @Validation.Required
  String getStartDate();
  void setStartDate(String value);

  @Description("Last day of precipitation data to upload. "
               + "Should be in the format \"YYYYMMD\".\n"
               + "If left blank, all data after the start date will "
               + "be included")
  @Default.String("")
  @Validation.Required
  String getEndDate();
  void setEndDate(String value);
}
