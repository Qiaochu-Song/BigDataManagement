//package proj02;

//import java.io.BufferedReader;
//import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
//import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SpatialJoin extends Configured implements Tool {
	// Two input files: schema
	// point: (x, y)
	// rectangles: (bottomLeft_x, bottomLeft_y, height, width)
    // private static ArrayList<Integer> window = new ArrayList<Integer>();
	
//    public void parseWindow(String w) {  //parse window info to the member variable
//        String [] temp = w.split(",");
//        for (String t:temp) {window.add(Integer.parseInt(t));}
//    }
    
	public static class pointMapper extends Mapper<Object, Text, NullWritable, Text> {
		
		@Override
		public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
			// get the window string from config XML and parse into an int array
			Configuration config = context.getConfiguration();
			String w = config.get("window");
			String[] tmp = w.split(",");
			int[] window = {0,0,0,0};
			for (int i=0; i<4; i++) {
			    window[i] = Integer.parseInt(tmp[i]);
			}
			// tell if the point is in the window
			String[] p = value.toString().split(",");
			int x = Integer.parseInt(p[0]);
			int y = Integer.parseInt(p[1]);
			// if in the window, write k = null and v = "p,x,y"
			if (x >= window[0] && x<= (window[0]+window[3]) && y >= window[1] && y <= (window[1]+window[2])) {
				context.write(NullWritable.get(), new Text("p," + x + "," + y));  //use "p" to indicate this is a point
			}
			
		}
	}
	
    public static class rectMapper extends Mapper<Object, Text, NullWritable, Text> {
    	@Override
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
    		// get the window string from config XML and parse into an int array
			Configuration config = context.getConfiguration();
			String w = config.get("window");
			String[] tmp = w.split(",");
			int[] window = {0,0,0,0};
			for (int i=0; i<4; i++) {window[i] = Integer.parseInt(tmp[i]);}
    		
			//parse the rectangle data
			String[] r = value.toString().split(",");
			int x1 = Integer.parseInt(r[0]); // left x
			int y1 = Integer.parseInt(r[1]);  // bottom y
			int x2 = x1 + Integer.parseInt(r[3]);  // right x
			int y2 = y1 + Integer.parseInt(r[2]); // up y
			
			//if rect intersect the window, write k = null and v = "r,BL_x,BL_y,h,w"
    		if (!(x2<window[0] || x1> (window[0]+window[3]) || y2 < window[1] || y1 > (window[1]+window[2]))) {
				context.write(NullWritable.get(), new Text("r," + x1+","+y1+","+Integer.parseInt(r[2])+","+Integer.parseInt(r[3])));  //use "r" to indicate this is a rectangle
			}
    	}
	}
    
    public static class spatialReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
		
		@Override
		public void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			// separate point and rectangle data
			ArrayList<int[]> points = new ArrayList<int[]>();
			ArrayList<int[]> rects = new ArrayList<int[]>();
			while (iter.hasNext()) {
				Text t = new Text(iter.next());
				String[] s = t.toString().split(",");
				if (s[0].equals("p")) {
					points.add(new int[]{Integer.parseInt(s[1]), Integer.parseInt(s[2])});
				}
				else if (s[0].equals("r")){  //format: x1,y1,h,w
                    rects.add(new int[]{Integer.parseInt(s[1]), Integer.parseInt(s[2]),Integer.parseInt(s[3]),Integer.parseInt(s[4])});
				}
			}
			// For each rect, scan all points, write to context if the point is in the rect
			for(int[] p: points) {
				int x = p[0];
				int y = p[1];
				for (int[] r: rects) {  // format x,y,h,w
				    if (x >= r[0] && x <= (r[0]+r[3]) && y>= r[1] && y <= (r[1]+r[2])){
				        context.write(NullWritable.get(), new Text("p:" + p[0]+","+p[1] + ";" + "r:" + r[0]+","+r[1]+","+r[2]+","+r[3]));
				    }
				}
			}
		}
    }
			
    
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 4) {
			System.out.println("Usage:  <pointFile> <rectFile> <outFolder> <window_int:[BL_x,BL_y,h,w]>");
			return -1;
		}
		//parse args, set file path
		Path pointInputPath = new Path(args[0]);
		Path rectInputPath = new Path(args[1]); 
		Path outputPath = new Path(args[2]);
		String window = args[3];
		//since we need to share the window data among all the mapper JVMs, we need to store it into configuration XML
		getConf().set("window", window);   // the window info is stored in config XML
		
		Job job = new Job(getConf(), "SpatialJoin"); 
		job.setJarByClass(getClass());
		
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//set mapper, reducer class
		MultipleInputs.addInputPath(job, pointInputPath, TextInputFormat.class, pointMapper.class);
		MultipleInputs.addInputPath(job, rectInputPath,TextInputFormat.class, rectMapper.class);
		
		job.setReducerClass(spatialReducer.class);
		//set K, V classes
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class); 
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
		
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SpatialJoin(), args); 
		System.exit(exitCode);
	}

}
