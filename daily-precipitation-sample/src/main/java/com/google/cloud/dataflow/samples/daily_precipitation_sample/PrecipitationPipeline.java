package com.google.cloud.dataflow.samples.daily_precipitation_sample;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.ImmutableList;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Preconditions;
import com.google.gdata.util.common.logging.FormattingLogger;
import com.google.gson.Gson;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Dataflow pipeline for NOAA precipitation data in a specified date range.
 * Data source can be found at http://water.weather.gov/precip/download.php
 *
 * <p>Pipeline for daily precipitation data can be found at
 * {@link DailyPrecipitationPipeline}
 *
 * <p>Example command line execution:
 * <br>{@code java -jar PrecipPipe.jar --startDate=20150101 --endDate=20150709 --project=myProject
 * --table=myProject:weather.precipitation --bucket=myBucket}
 *
 * <p>Run {@code java -jar PrecipPipe.jar --help} for more information.
 * <br>Include the "--help=PrecipitationOptions" flag for a list of pipeline-specific options.
 *
 * @author jsvangeffen
 */
public class PrecipitationPipeline implements Serializable {

  private static final long serialVersionUID = -2557657935008647947L;

  private static final Gson GSON = new Gson();

  private static final TableSchema SCHEMA = new TableSchema().setFields(
      new ImmutableList.Builder<TableFieldSchema>()
          .add(new TableFieldSchema().setName("Year").setType("STRING"))
          .add(new TableFieldSchema().setName("Month").setType("STRING"))
          .add(new TableFieldSchema().setName("Day").setType("STRING"))
          .add(new TableFieldSchema().setName("Lat").setType("FLOAT"))
          .add(new TableFieldSchema().setName("Lon").setType("FLOAT"))
          .add(new TableFieldSchema().setName("Precip").setType("FLOAT"))
          .build());

  private static final FormattingLogger LOG = new FormattingLogger(PrecipitationPipeline.class);

  private boolean appendMode;
  private String project;
  private String bucket;
  private String table;
  private String startDate;
  private String endDate;
  private String[] pipelineOptionsArgs;

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
    project = options.getProject();
    bucket = options.getBucket();
    table = options.getTable();
    startDate = options.getStartDate();
    endDate = options.getEndDate();
  }

  /**
   * Runs the pipeline, writing precipitation data for the date range specified
   * by the appropriate flags.
   */
  public void run() {
    appendToTable(startDate, endDate);
  }

  /**
   * Appends data from a file onto existing table.
   * A single file should represent the data for one day.
   * @param date Date of precipitation data to append.
   */
  public void appendToTable(final String date) {
    appendToTable(date, date);
  }

  /**
   * Appends data from several files onto existing table.
   * A single file should represent the data for one day.
   * @param dates Dates of precipitation data to append.
   */
  public void appendToTable(final List<String> dates) {
    appendToTable(ReadDataWithFileName.construct(project, bucket, dates));
  }

  /**
   * Appends data from all existing files in a GCS bucket within a specified date range.
   * A single file should represent the data for one day.
   */
  public void appendToTable(String startDate, String endDate) {
    appendToTable(ReadDataWithFileName.construct(project, bucket, startDate, endDate));
  }

  /**
   * Appends data from all existing files in a GCS bucket.
   * A single file should represent the data for one day.
   */
  public void appendToTable() {
    appendToTable(ReadDataWithFileName.construct(project, bucket));
  }

  /**
   * Appends data from files specified by the given reader.
   * @param reader Input reader that represents which data files should be pipelined.
   */
  public void appendToTable(ReadDataWithFileName reader) {
    WriteDisposition disposition = !appendMode
            ? WriteDisposition.WRITE_TRUNCATE : WriteDisposition.WRITE_APPEND;

    Pipeline appendToTable = Pipeline.create(options);

    DoFn<KV<String, String>, TableRow> appendFn = getAppendDoFn();

    SerializableFunction<KV<String, String>, Boolean> filterPredicate = getFilterPredicate();

    appendToTable.apply(reader)
                 .apply(Filter.by(filterPredicate).named("Filter Invalid JSON"))
                 .apply(ParDo.of(appendFn).named("Append to table"))
                 .apply(BigQueryIO.Write.named("Write Precipitation Data").to(table)
                     .withSchema(SCHEMA)
                     .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                     .withWriteDisposition(disposition));

    try {
      appendToTable.run();
    } catch (RuntimeException pipelineException) {
      // May throw a RuntimeException if there is no precipitation data for this day (or days),
      // which would be caused by a FileNotFoundException.
      if (pipelineException.getCause() instanceof FileNotFoundException) {
        LOG.warningfmt(pipelineException,
            "Exception running pipeline. Some input file(s) were not found.");
      } else {
        throw pipelineException;
      }
    }

  }

  private DoFn<KV<String, String>, TableRow> getAppendDoFn() {
    return new DoFn<KV<String, String>, TableRow>() {

        private static final long serialVersionUID = 2L;

        @Override
        public void processElement(ProcessContext c) {
          String inputRow = c.element().getValue().trim();
          if (inputRow.length() <= 1) {
            return;
          }

          // Trim the trailing comma on a row of data if it exists.
          // If there is no comma (e.g. on the last row of data), leave the row alone.
          if (inputRow.charAt(inputRow.length() - 1) == ',') {
            inputRow = inputRow.substring(0, inputRow.length() - 1).trim();
          }

          String fileName = c.element().getKey().toString();

          TableRow outputRow = new TableRow();
          PrecipitationRow dataRow = GSON.fromJson(inputRow, PrecipitationRow.class);
          String[] ymd = getDateFromString(fileName);

          outputRow.put("Year", ymd[0]);
          outputRow.put("Month", ymd[1]);
          outputRow.put("Day", ymd[2]);
          outputRow.put("Lat", dataRow.properties.lat);
          outputRow.put("Lon", dataRow.properties.lon);
          outputRow.put("Precip", dataRow.properties.globvalue);

          c.output(outputRow);
        }

    };
  }
  
  private SerializableFunction<KV<String, String>, Boolean> getFilterPredicate() {
    return new SerializableFunction<KV<String, String>, Boolean>() {

        private static final long serialVersionUID = 0;

        @Override
        public Boolean apply(KV<String, String> row) {
          // Should be true only when row is an expected row in the data file
          // NOTE: If the format of the data files changes in the future,
          //       this may need to change as well.
          return row.getValue().matches("\\{\\s*\"type\"\\s*:\\s*\"Feature\".*");
        }
    };
  }

  /**
   * A utility method for dealing with the start_date and end_date flags.
   * @param dateString String in the format ".*YYYYMMDD.*",
   * where ".*" represents any number of characters.
   * <p>Note that there should be only one continuous substring of 8 digits,
   * and NO substrings of 9 or more digits.
   * @return A 3-element array in the format { "YYYY", "MM", "DD" },
   * <br>representing year, month, and day,
   * or {@code null} if no match was found.
   */
  public static String[] getDateFromString(String dateString) {
    Pattern pattern = Pattern.compile(".*(?<YEAR>\\d{4})(?<MONTH>\\d{2})(?<DAY>\\d{2}).*");
    Matcher matcher = pattern.matcher(dateString);
    try {
      Preconditions.checkState(matcher.matches());
    } catch (IllegalStateException noMatch) {
      // No match was found.
      LOG.warningfmt(noMatch, "No match found for \"YYYYMMDD\" in \"" + dateString + "\"");
      throw noMatch;
    }
    return new String[] {
        matcher.group("YEAR"),
        matcher.group("MONTH"),
        matcher.group("DAY"),
    };
  }

  public static void main(String[] args) {
    PrecipitationPipeline pipeline = new PrecipitationPipeline(args);
    pipeline.run();
  }

}
