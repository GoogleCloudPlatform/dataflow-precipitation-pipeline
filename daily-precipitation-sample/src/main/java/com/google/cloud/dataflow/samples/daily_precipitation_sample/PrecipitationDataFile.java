package com.google.cloud.dataflow.samples.daily_precipitation_sample;


/**
 * @author stephanmeyn@google.com
 *
 * Object representation of a data download from noaa.
 * Created for GSON, matches JSON file representation.
 */

public class PrecipitationDataFile {
	static class MetaData{
		public int offset;
		public int count;
		public int limit;
	}
  static class HeaderData {
	  public MetaData resultset;
  }
  
  static class PrecipitationRecord{
	  public String date;
	  public String datatype;
	  public String station;
	  public String attributes;
	  public Double value;
  }
  
     public HeaderData metadata;
     public PrecipitationRecord results[];
}
