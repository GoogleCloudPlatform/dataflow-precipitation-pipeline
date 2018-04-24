# Dataflow Precipitation Sample

## About

Both Google Dataflow and Google BigQuery can be tricky to use, let alone combining the two. This example pipeline uses both technologies extensively and implements several features (both simple and complicated) that you may need in your own project. Feel free to explore this example as you like. We hope it may serve some small part in furthering your endevours. 

### Data Source

All data comes from NOAA and can be found at https://www.ncdc.noaa.gov/cdo-web/
 
To download a data set use:

```
curl -H "token:<WEBTOKEN>" "https://www.ncdc.noaa.gov/cdo-web/api/v2/data?startdate=2010-05-01&enddate=2010-05-31&s&units=metric&datasetid=PRECIP_HLY&limit=1000&offset=1000" >d4.json
```
*you need to get an Web Token first*

see here: https://www.ncdc.noaa.gov/cdo-web/token



## Usage

### Credentials

If you do not already have Google cloud credentials setup, you'll need to install gcloud and run the command:

    $ gcloud auth login

This pipeline uses the default Google credentials.
You can find more about setting up Google credentials at https://developers.google.com/identity/protocols/application-default-credentials 

### Execution

To run this pipeline, simply run the command:

    $ java -jar PrecipPipe.jar  --project=myProject \
    --table=myProject:weather.precipitation --bucket=myBucket \
    --inputFilePattern=gs://dataflow/input/*.json \
    --bigQueryLoadingTemporaryDirectory=gs://dataflow/BigQueryWriteTemp
 

For a full list of options, run:

    $ java -jar PrecipPipe.jar --help

Include the "--help=PrecipitationOptions" flag for a list of pipeline-specific options.

## Details
The example demonstrates several techniques:
* How to define pipeline options
* Composite transforms
* How to use the Filesystem API to access files via a URL
* How to introduce flexibility into the pipeline by parameteriz

### PrecipitationPipeline.java

This class describes the pipeline. the method 

    public void appendToTable(CollectPrecipitationdataFiles reader)
  
implements the actual pipeline (called processWeatherDataP). It takes as an input a transform that defines the data input. 
Doing so allows the use of different data sources, depending on invocation.

the pipeline has 3 major transformation steps:

* Read Precipitation Data : extracts the data from the sources
* Append to table   : creates a set of table rows
* Write to BigQuery  : writes the data to BQ

### CollectPrecipitationDataFiles.java

The data source transform supplied to the pipeline is defined in this class. It has a constructor named 'of'
which takes a file pattern of the format of  

	gs://sub/dir/path/*.json

*Note*: the pattern does not have to be a GCS gs:// style url. If the pipeline is run in a runner that has access to
the local file system then the url may refer to the file system

This class is an example of creating a composite transform. It retrieves the pipeline and applies
several transforms.

* *Match File(s)* uses ```FileIO.match()``` to insert filepath records into the pipeline
* *ReadFiles*  takes the filepaths and reads in the filecontents as a single string. This is implemented in the ```PreipitationDataReader``` class
* *ParseJSON*  parses string as a JSON Object into an instance of class ```PrecipitationDataFile```
* *split to records*  extracts the actual precipitation data records from the data file record 
   
###PrecipitationOptions.java

this class defines the required options for the applications.
the options are 

* *append*
* *project*
* *InputFilePattern*
* *bigQueryLoadingTemporaryDirectory*
* *getTable* 


## License

This library is licensed under Apache 2.0. Full license text is
available in [LICENSE](LICENSE.txt).

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md).

## Support

For support on BigQuery and Dataflow, please submit questions tagged with
[`google-bigquery`](http://stackoverflow.com/questions/tagged/google-bigquery)
and [`google-cloud-dataflow`](http://stackoverflow.com/questions/tagged/google-cloud-dataflow)
on StackOverflow.

For issues, please [submit issues](https://github.com/GoogleCloudPlatform/dataflow-precipitation-pipeline/issues) here on this project page.
