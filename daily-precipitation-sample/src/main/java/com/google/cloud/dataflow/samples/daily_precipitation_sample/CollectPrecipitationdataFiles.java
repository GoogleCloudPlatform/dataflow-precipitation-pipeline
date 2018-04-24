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

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
/**
 *  A PTransform that returns records that represent  single data download files 
 *
 * @author Stephanmeyn
 */
public class CollectPrecipitationdataFiles extends
    PTransform<PInput, PCollection< PrecipitationDataFile.PrecipitationRecord>> {

  private static final long serialVersionUID = 6410869694327602135L;

  private String inputFilePattern;

  private CollectPrecipitationdataFiles(String inputFilePattern) {
    this.inputFilePattern = inputFilePattern; 
  }

/*
  @Override
  public Coder<KV<String, String>> getDefaultOutputCoder() {
    return KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());
  }*/

	@Override
	public PCollection<PrecipitationDataFile.PrecipitationRecord> expand(PInput input) {
		
		Pipeline pipeline = input.getPipeline();
		return pipeline.apply("Match File(s)", FileIO.match().filepattern(inputFilePattern))
				.apply("Read files", ParDo.of(new PrecipitationDataReader()))
				.apply("Parse JSON", ParseJsons.of(PrecipitationDataFile.class)).setCoder(AvroCoder.of(PrecipitationDataFile.class))
				.apply("split to records", FlatMapElements
						.via(new SimpleFunction<PrecipitationDataFile, List<PrecipitationDataFile.PrecipitationRecord>>() {
							public List<PrecipitationDataFile.PrecipitationRecord> apply(PrecipitationDataFile fileData) {
								return Arrays.asList(fileData.results);
							}
						})
						).setCoder(AvroCoder.of(PrecipitationDataFile.PrecipitationRecord.class));

	}


  /**
   * Construct a new CollectPrecipitationdataFiles object that reads all precipitation files
   * that match the input file pattern.
   */
  public static CollectPrecipitationdataFiles of (String inputFilePattern) {
    return new CollectPrecipitationdataFiles(inputFilePattern );
  }

  
}
