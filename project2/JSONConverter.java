import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configured; 
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.LineReader;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
//import org.apache.hadoop.mapreduce.FileSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class JSONConverter extends Configured implements Tool {
	// read many lines from the input json file
	// until it gets a complete record
	// convert these lines to a list of a single CSV line, pass it to the map function
	// each input to the mapper: field values, comma separated,
	// i.e. use JSONInpuFormat to parse the field values, concat to CSV line
	// make it only 5 mappers
	// i.e. divide the file (indep. from  block size) into 5 splits.

	// can extend "NLineInputFormat.get" to read 15 lines as one record/split
	// can use "LineRecordReader.getSplitsFromFile" to get 15 lines
	// usage: LineRecordReader lrr = new LineRecordReader(); lrr.initialize(inputSplit, context);
	// if (!lrr.nextKeyValue()){[the file is read to the end]};  
	// lrr.getCurrentValue().toString(); [then parse it]
	
	// note can directly use public static void setNumLinesPerSplit(Job job, int numLines)
    // of NLineInputFormat to Set the number of lines per split
	// note many packages unimported
	// note the JSONInputFormat key can be NullWritable
	
	public static class JSONInputFormat extends FileInputFormat<NullWritable,Text> {
		
		@Override
		public JSONRecordReader createRecordReader(InputSplit is,TaskAttemptContext context) 
				throws IOException, InterruptedException {
			 context.setStatus(is.toString());
			 JSONRecordReader jrr = new JSONRecordReader();
	         jrr.initialize(is, context);
	         return jrr;
		}
		
//		public static void setMaxInputSplitSize(Job job, long size) {
//            job.getConfiguration().setLong("mapred.max.split.size", size);
//        }
		
		@Override
        public List<InputSplit> getSplits(JobContext job) throws IOException {
			List<InputSplit> splits = new ArrayList<InputSplit>();
		    for (FileStatus status : listStatus(job)) {
				Path fileName = status.getPath();
				FileSystem  fs = fileName.getFileSystem(job.getConfiguration());
				LineReader lr = null;
				try {
				    FSDataInputStream in  = fs.open(fileName);  // open the file as "in"
				    lr = new LineReader(in, job.getConfiguration());  // we will use this lr to read file "in" line by line
					Text line = new Text();
					int numLines = 0;
					// count the total #lines in the file
					while (lr.readLine(line) > 0) {
					    numLines++;    
					}
                                        System.out.println("numLines"+numLines);
					// #records = numLines/15; # recordsPerSplit = #records/5
					// add 1 rec per split to ensure all records can be read
					int recordsPerSplit = numLines/15/5 + 1;  // #records = numLines/15; #recordsPerSplit = #records/5; 
					int numLinesPerSplit = 15 * recordsPerSplit;
	                splits.addAll(NLineInputFormat.getSplitsForFile(status, job.getConfiguration(), numLinesPerSplit));	
				} finally {
					if (lr != null) {lr.close();}
				  }
		    }
                    
				return splits;      
        }
		
        // define the record reader for the input format
	    public static class JSONRecordReader extends RecordReader<NullWritable,Text> {
	        private NullWritable key;
	        private Text value;
	        private LineRecordReader lrr;
	        	        
	        @Override
	        public void initialize(InputSplit is, TaskAttemptContext context)throws IOException, InterruptedException {
	        	lrr = new LineRecordReader();
	            lrr.initialize(is,context);
	        }
	        
	        @Override
	        public boolean nextKeyValue() throws IOException, InterruptedException {
	        	// parse the json record from fileSplit or inputSplit List, 
	        	// rewrite the value format as a value csv line, which will be turn into map func
	        	StringBuilder valueStr = new StringBuilder();
	        	
	        	// use LineRecordReader to read each line, and convert multiple lines into one record
	        	
	        	while(true) {
                            if (!lrr.nextKeyValue()) {  // if already done reading all splits, return false
	                        key = null;
	                        value = null;
	                        return false;
	                    }
		            String nextLine = lrr.getCurrentValue().toString();
		            nextLine.replaceAll("\\s+", "");  // delete all beginning spaces
		            // if this line is content line, append its value field to valueStr
		            if (!nextLine.contains("{") && !nextLine.contains("}") && !nextLine.equals("")) { 
                            //if (nextLine.contains(":")){
		            	valueStr.append(nextLine.split(":")[1]);
		            }
		            // if this is end line, break the loop, then set the value and return
		            if (nextLine.contains("}")){break;}        
	        	}
	        	key = NullWritable.get();
	        	value = new Text(valueStr.toString());  // now the output should be "v1, v2,v3,..."
	        	return true; 
             }
	        @Override
                public float getProgress() throws IOException {
                    return lrr.getProgress();
                }
             
	        @Override
	        public void close() throws IOException {
	            lrr = null;
	            key = null;
	            value = null;
	        }
	        
	        @Override
	        public NullWritable getCurrentKey() throws IOException,InterruptedException {
	            return key;
	        }

	        @Override
	        public Text getCurrentValue() throws IOException, InterruptedException {
	            return value;
	        }
	    }
    }
	// aggregates the records based on the “Flag” field, 
	// for each flag value, report the maximum and minimum elevation value
	
	public static class JSONMapper extends Mapper<Object,Text, Text, Text>{
		@Override
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			//key = value of flag field (the 6th idx=5 col in csv); value = elevation value (9th, idx=8)
			String[] line = value.toString().split(",");
			context.write(new Text(line[5]), new Text(line[8]));
		}
	}
	
	public static class JSONReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			int maxElevation = 0;
			int minElevation = Integer.MAX_VALUE;
			while (iter.hasNext()) {
				String r = new Text(iter.next()).toString().replaceAll("[^0-9]","");
				int e = Integer.parseInt(r);
				if (e>maxElevation) {maxElevation = e;}
				if (e<minElevation) {minElevation = e;}
			}
			context.write(key, new Text(maxElevation+","+minElevation));
		}
	}
	
	public int run(String[] args)  throws Exception {
		if (args.length != 2) {
			System.out.println("Usage:  <json input> <output>");
			return -1;
		}
		Path jsonPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
	    Job job = new Job(getConf(), "JSONConverter"); 
		job.setJarByClass(getClass());
                job.setInputFormatClass(JSONInputFormat.class);
		FileInputFormat.addInputPath(job, jsonPath);  // use customized input format
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setMapperClass(JSONMapper.class);
		job.setReducerClass(JSONReducer.class);
		//set k, v class
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode = ToolRunner.run(new JSONConverter(), args); 
		System.exit(exitCode);
	}
}
