/*
 * Copyright (C) 2015-2018 Google Inc.
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

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions.DefaultProjectFactory;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Extra options for {@link PrecipitationPipeline}.
 *
 * @author jsvangeffen, stephanmeyn
 */
public interface PrecipitationOptions extends PipelineOptions {


  @Description("Set to true when appending to existing table, "
               + "false when overriding existing table")
  @Default.Boolean(true)
  @Validation.Required
  boolean getAppend();
  void setAppend(boolean value);

  @Description("Existing Google Cloud project to work with.")
  @Default.InstanceFactory(DefaultProjectFactory.class)
  @Validation.Required
  String getProject();
  void setProject(String value);
  
  @Description("File pattern used to match files. Ex. gs://sub/dir/*.json")
  @Validation.Required
  String getInputFilePattern();
  void setInputFilePattern(String value);

  @Validation.Required
  @Description("Temporary directory for BigQuery loading process")
  ValueProvider<String> getBigQueryLoadingTemporaryDirectory();
  void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);
  
  @Description("Fully-qualified BigQuery table to update. "
               + "Should be in the format \"project:dataset.table\".")
  @Default.String("myProject:weather.us_precipitation")
  @Validation.Required
  String getTable();
  void setTable(String value);

}
