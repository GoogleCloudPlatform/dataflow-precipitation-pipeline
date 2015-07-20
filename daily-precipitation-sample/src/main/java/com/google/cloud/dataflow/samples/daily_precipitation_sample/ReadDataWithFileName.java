package com.google.cloud.dataflow.samples.daily_precipitation_sample;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.WithKeys;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PInput;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * A PTransform that returns key-value pairs of data,
 * mapping single rows to the files they came from.
 * Provides a way to associate rows of data with their assiciated files.
 *
 * @author jsvangeffen
 */
public class ReadDataWithFileName extends
    PTransform<PInput, PCollection<KV<String, String>>> {

  private static final long serialVersionUID = 6410869694327602135L;

  private static final String ACCEPTED_FILE_TEMPLATE = "^(.*/)?precip_(\\d{8})\\.json$";
  private static final String FILE_DATE_FROM_TEMPLATE = "$2";

  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  private Iterable<String> files;

  private ReadDataWithFileName(Iterable<String> files) {
    this.files = files;
  }

  @Override
  public Coder<KV<String, String>> getDefaultOutputCoder() {
    return KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());
  }

  @Override
  public PCollection<KV<String, String>> apply(PInput input) {
    Pipeline pipeline = input.getPipeline();

    // Create one TextIO.Read transform for each data file
    // and add its output to a PCollectionList
    PCollectionList<KV<String, String>> filesToLines = PCollectionList.empty(pipeline);

    for (final String fileLocation : files) {
      PTransform<PInput, PCollection<String>> inputSource
          = TextIO.Read.from(fileLocation)
              .named("TextIO.Read(" + fileLocation + ")");

      PCollection<KV<String, String>> oneFileToLines = pipeline
          .apply(inputSource)
          .apply(WithKeys.<String, String>of(fileLocation));

      filesToLines = filesToLines.and(oneFileToLines);
    }

    return filesToLines.apply(Flatten.<KV<String, String>> pCollections())
                       .setCoder(getDefaultOutputCoder());
  }

  /**
   * Construct a new ReadDataWithFileName object that reads all precipitation files
   * fron the specified bucket.
   * @param startDate Beginning date of data files, in the format "YYYYMMDD"
   * @param endDate Ending date of data files, in the format "YYYYMMDD"
   */
  public static ReadDataWithFileName construct(String project, String bucket,
      String startDate, String endDate) {
    checkDate(startDate, "startDate");
    checkDate(endDate, "endDate");

    return new ReadDataWithFileName(
        getPrecipitationFiles(project, bucket, startDate, endDate));
  }

  /**
   * Construct a new ReadDataWithFileName object that reads all precipitation files
   * from the specified bucket.
   */
  public static ReadDataWithFileName construct(String project, String bucket,
      Iterable<String> dates) {
    return new ReadDataWithFileName(
        getPrecipitationFiles(project, bucket, dates));
  }

  /**
   * Construct a new ReadDataWithFileName object that reads all precipitation files
   * from the specified bucket.
   */
  public static ReadDataWithFileName construct(String project, String bucket) {
    return new ReadDataWithFileName(
        getPrecipitationFiles(project, bucket, "", ""));
  }


  /**
   * Get all precipitation files in a specified bucket that pertain to the list of dates specified.
   * @return A set of fully qualified file names for all precipitation data files.
   * All names are in the format "gs://sub/dir/.../precip_YYYYMMDD.json"
   */
  private static HashSet<String> getPrecipitationFiles(String project, String bucket,
      Iterable<String> dates) {
    HashSet<String> matchingFiles = new HashSet<>();
    HashSet<String> hashDates = new HashSet<>();

    HashSet<String> precipFiles =
        getPrecipitationFiles(project, bucket, "", "");

    for (String date : dates) {
      hashDates.add(date);
    }

    for (String file : precipFiles) {
      String date = file.replaceAll(ACCEPTED_FILE_TEMPLATE, FILE_DATE_FROM_TEMPLATE);
      if (hashDates.contains(date)) {
        matchingFiles.add(file);
      }
    }

    return matchingFiles;
  }

  /**
   * Get all precipitation files in a specified bucket within the specified date range.
   * @return A set of fully qualified file names for all precipitation data files.
   * All names are in the format "gs://sub/dir/.../precip_YYYYMMDD.json"
   */
  private static HashSet<String> getPrecipitationFiles(String project, String bucket,
      String startDate, String endDate) {
    HashSet<String> files = new HashSet<>();

    // Prevents duplicate data files for the same date.
    HashSet<String> visitedFiles = new HashSet<>();

    try {
      HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
      GoogleCredential credential = GoogleCredential.getApplicationDefault();

      Collection<String> bigqueryScopes = BigqueryScopes.all();
      if (credential.createScopedRequired()) {
        credential = credential.createScoped(bigqueryScopes);
      }

      Storage client = new Storage.Builder(httpTransport, JSON_FACTORY, credential)
          .setApplicationName(project).build();

      // Get the contents of the bucket.
      Storage.Objects.List listObjects = client.objects().list(bucket);
      com.google.api.services.storage.model.Objects objects;
      do {
        objects = listObjects.execute();
        List<StorageObject> items = objects.getItems();
        if (items == null) {
          break;
        }
        for (StorageObject object : items) {
          String fileName = PathUtil.basename(object.getName());
          if (matchesFileTemplate(fileName) && isInRange(fileName, startDate, endDate)
              && !visitedFiles.contains(fileName)) {
            visitedFiles.add(fileName);
            files.add("gs://" + PathUtil.join(bucket, object.getName()));
          }
        }
        listObjects.setPageToken(objects.getNextPageToken());
      } while (objects.getNextPageToken() != null);

    } catch (IOException | GeneralSecurityException e) {
      throw new RuntimeException("Exception while constructing ReadDataWithFileName reader.", e);
    }

    return files;
  }

  private static boolean matchesFileTemplate(String fileName) {
    return fileName.matches(ACCEPTED_FILE_TEMPLATE);
  }

  /**
   * Checks date parameter.
   * @param date String date to check.
   */
  private static void checkDate(String date, String argName) {
    if (date == null) {
      throw new IllegalArgumentException("Argument " + argName + " cannot be null");
    }
    if (date.isEmpty()) {
      return;
    }
    DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    try {
      dateFormat.parse(date);
      return;
    } catch (ParseException e) {
      throw new IllegalArgumentException("Exception parsing argument " + argName, e);
    }
  }

  /**
   * Checks if the date pertaining to the name of a file is within a specified range.
   * @param startDate Starting date of the range, in format "YYYYMMDD". Can also be set
   * to "" to match all files before the end date.
   * @param endDate Ending date of the range, in format "YYYYMMDD". Can also be set
   * to "" to match all files after the start date.
   * @return {@code true} if the date is within the range, {@code false} otherwise.
   * If both startDate and endDate are "", will return {@code true}.
   */
  private static boolean isInRange(String fileName, String startDate, String endDate) {
    if (startDate.isEmpty() && endDate.isEmpty()) {
      return true;
    }

    // Trim the "precip_" and ".json" from fileName.
    String fileDate = fileName.replaceAll(ACCEPTED_FILE_TEMPLATE, FILE_DATE_FROM_TEMPLATE);

    DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    Calendar calFile = Calendar.getInstance();
    Calendar calStart = Calendar.getInstance();
    Calendar calEnd = Calendar.getInstance();

    try {
      if (!startDate.isEmpty()) {
        calStart.setTime(dateFormat.parse(startDate));
      }
      if (!endDate.isEmpty()) {
        calEnd.setTime(dateFormat.parse(endDate));
      }
    } catch (ParseException e) {
      // A ParseException for startDate and endDate would have already been caught.
      throw new RuntimeException("Date ranges are malformated.", e);
    }

    try {
      calFile.setTime(dateFormat.parse(fileDate));
    } catch (ParseException e) {
      throw new RuntimeException("ParseException for date on file name: " + fileName, e);
    }

    if (startDate.isEmpty()) {
      return calFile.compareTo(calEnd) <= 0;
    } else if (endDate.isEmpty()) {
      return calStart.compareTo(calFile) <= 0;
    } else {
      return calStart.compareTo(calFile) <= 0 && calFile.compareTo(calEnd) <= 0;
    }
  }
}
