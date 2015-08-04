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
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Dataflow pipeline for NOAA daily precipitation data
 * Data source can be found at http://water.weather.gov/precip/download.php
 *
 * <p>Example command line execution:
 * <br>{@code java -jar DailyPrecipPipe.jar --date=20150621 --project=myProject
 * --table=myProject:weather.precipitation --bucket=myBucket}
 *
 * <p>Run {@code java -jar DailyPrecipPipe.jar --help} for more information.
 * <br>Include the "--help=DailyPrecipitationOptions" flag for a list of pipeline-specific options.
 * @author jsvangeffen
 */
public class DailyPrecipitationPipeline implements Serializable {

  private static final long serialVersionUID = -810754744815157281L;

  private static interface DailyPrecipitationOptions extends PrecipitationOptions {
    @Description("Date of precipitation data to upload. Should be in the format \"YYYYMMD\"")
    @Default.String("")
    @Validation.Required
    String getDate();
    void setDate(String value);
  }

  private String date;
  private String[] pipelineOptionsArgs;

  /**
   * @param pipelineOptionsArgs Command line arguments for the pipeline.
   * <br>Each options should be in the format "--flag=value".
   * <br>For a list of available daily precipitation flags, include the
   * "--help=DailyPrecipitationOptions" flag.
   * <br>For a list of all other flags, include the "--help" flag.
   */
  public DailyPrecipitationPipeline(String[] pipelineOptionsArgs) {
    this.pipelineOptionsArgs = pipelineOptionsArgs;

    // Load date from pipeline options.
    PipelineOptionsFactory.register(DailyPrecipitationOptions.class);
    DailyPrecipitationOptions options = PipelineOptionsFactory.fromArgs(pipelineOptionsArgs)
        .withValidation().create().as(DailyPrecipitationOptions.class);

    date = options.getDate();

    // Set the default date to yesterday if date wasn't set manually.
    if (date.isEmpty()) {
      DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
      Calendar cal = Calendar.getInstance();
      cal.add(Calendar.DATE, -1); // Represents the previous day.

      this.date = dateFormat.format(cal.getTime());
    }
  }

  /**
   * Uploads the precipitation data for a single day.
   * The specific day can be specified through command line flags
   * or in the constructor.
   */
  public void run() {
    PrecipitationPipeline pipeline = new PrecipitationPipeline(pipelineOptionsArgs);
    pipeline.appendToTable(date);
  }

  public static void main(String[] args) {
    DailyPrecipitationPipeline pipeline = new DailyPrecipitationPipeline(args);
    pipeline.run();
  }

}
