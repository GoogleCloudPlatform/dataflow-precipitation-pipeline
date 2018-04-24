package com.google.cloud.dataflow.samples.daily_precipitation_sample;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.DoFn;
import java.nio.charset.StandardCharsets;
import com.google.common.io.CharStreams;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * DoFn to process a single data file. 
 * Input: a file name
 * output: the content of the file
 * @author stephanmeyn
 *
 */
public class PrecipitationDataReader extends DoFn<MatchResult.Metadata, String>{

	private static final long serialVersionUID = 2798487542524640402L;

	/**
	 * read a line with a file name. Open the file and extract the json
	 * @param context
	 */
    @ProcessElement
    public void processElement(ProcessContext context) {
    		ResourceId inputFile = context.element().resourceId();
    		
    		try {
    			ReadableByteChannel readerChannel = FileSystems.open(inputFile) ;
    			Reader reader =  Channels.newReader(readerChannel,  StandardCharsets.UTF_8.name());
    			String content = CharStreams.toString(reader);
    			context.output(content);
    		} catch(IOException ex) {
    			throw new UncheckedIOException(ex);
    		}
    }
}
