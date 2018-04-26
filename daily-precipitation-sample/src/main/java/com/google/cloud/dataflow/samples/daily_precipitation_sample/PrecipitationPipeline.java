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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.repackaged.com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import com.google.gdata.util.common.logging.FormattingLogger;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Dataflow pipeline for NOAA precipitation data in a specified date range.
 * Data source can be found at http://water.weather.gov/precip/download.php
 *
 * <p>Pipeline for daily precipitation data can be found at
 * {@link DailyPrecipitationPipeline}
 *
 * <p>Example command line execution:
 * <br>{@code java -jar PrecipPipe.jar 
 *                 --table=dataflow_demo.precipitation  
 *                 --inputFilePattern=gs://dataflow/input/*.json 
 *                 --bigQueryLoadingTemporaryDirectory=gs://dataflow/temp
 *   }
 *
 * <p>Run {@code java -jar PrecipPipe.jar --help} for more information.
 * <br>Include the "--help=PrecipitationOptions" flag for a list of pipeline-specific options.
 *
 * @authors  stephanmeyn@google.com
 */
public class PrecipitationPipeline implements Serializable {

  private static final long serialVersionUID = -2557657935008647947L;

  private static final TableSchema SCHEMA = new TableSchema().setFields(
      new ImmutableList.Builder<TableFieldSchema>()
          .add(new TableFieldSchema().setName("Date").setType("DATETIME"))
          .add(new TableFieldSchema().setName("Station").setType("STRING")) 
          .add(new TableFieldSchema().setName("Precip").setType("FLOAT"))
          .add(new TableFieldSchema().setName("DateAdded").setType("DATETIME"))
          .build());

  private static final FormattingLogger LOG = new FormattingLogger(PrecipitationPipeline.class);

  private boolean appendMode;
  private String table;
  private String[] pipelineOptionsArgs;
  private String inputFilePattern;
  private transient PrecipitationOptions options;

  /**
   * @param pipelineOptionsArgs Command line arguments for the pipeline.
   * <br>Each options should be in the format "--flag=value".
   * <br>For a list of available daily precipitation flags, include the
   * "--help=PrecipitationOptions" flag.
   * <br>For a list of all other flags, include the "--help" flag.
   */
  public PrecipitationPipeline(String[] pipelineOptionsArgs) {
    this.pipelineOptionsArgs = pipelineOptionsArgs;
    loadOptions();
  }

  private void loadOptions() {
    PipelineOptionsFactory.register(PrecipitationOptions.class);

    options = PipelineOptionsFactory.fromArgs(pipelineOptionsArgs)
        .withValidation().create().as(PrecipitationOptions.class);

    appendMode = options.getAppend();
    table = options.getTable();
    inputFilePattern = options.getInputFilePattern();
  }

  /**
   * Runs the pipeline, writing precipitation data for the date range specified
   * by the appropriate flags.
   */
  public void run() {
    appendToTable();
  } 


  /**
   * Appends data from all existing files that match the inputFilePattern.
   * A single file should represent the data for one day.
   */
  public void appendToTable() {
    appendToTable(CollectPrecipitationdataFiles.of(inputFilePattern));
  }

  /**
   * Appends data from files specified by the given reader.
   * @param reader Input reader that represents which data files should be pipelined.
   */
  public void appendToTable(CollectPrecipitationdataFiles reader) {
	  
    WriteDisposition disposition = !appendMode
            ? WriteDisposition.WRITE_TRUNCATE : WriteDisposition.WRITE_APPEND;

    Pipeline processWeatherDataP = Pipeline.create(options);
     
    DoFn< PrecipitationDataFile.PrecipitationRecord, TableRow> appendFn = getAppendDoFn();

  
 
    processWeatherDataP.apply("Read Precipitation Data", reader)
                 .apply("Append to table", ParDo.of(appendFn))
                 .apply("Write to BigQuery",  BigQueryIO.writeTableRows().to(table)
                     .withSchema(SCHEMA)
                     .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                     .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                     .withWriteDisposition(disposition));

    try {
      processWeatherDataP.run();
    } catch (RuntimeException pipelineException) {
      // May throw a RuntimeException 
      // which would be caused by a FileNotFoundException.
      if (pipelineException.getCause() instanceof FileNotFoundException) {
        LOG.warningfmt(pipelineException,
            "Exception running pipeline. Some input file(s) were not found.");
      } else {
        throw pipelineException;
      }
    }

  }

  @SuppressWarnings("serial")
private DoFn<PrecipitationDataFile.PrecipitationRecord, TableRow> getAppendDoFn() {
    return new DoFn< PrecipitationDataFile.PrecipitationRecord, TableRow>() {

        @ProcessElement
        public void processElement(ProcessContext c) {
 
          PrecipitationDataFile.PrecipitationRecord dataRow = c.element();
 
          TableRow outputRow = new TableRow();
          DateTimeFormatter formatter = DateTimeFormatter.ofPattern( "yyyy-MM-dd'T'HH:mm:ss");
          ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
          outputRow.put("Date", dataRow.date);
          outputRow.put("Station", dataRow.station); 
          outputRow.put("Precip", dataRow.value);
          outputRow.put("DateAdded",  now.format(formatter));

          c.output(outputRow);
        }

    };
  }
  
 

  public static void main(String[] args) {
    PrecipitationPipeline pipeline = new PrecipitationPipeline(args);
    pipeline.run();
  }

}
