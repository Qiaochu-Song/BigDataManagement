//package proj02;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans2 extends Configured{
	// Two input files: schema
	// Point: (x, y)
	// Centroids: in dir "/Kmeans/iter_#", the 1st one is created manually beforehand
	// The centroid file as distributed cache to all mappers, saved in "centroids"
	
	public static class kMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
		// the mapper is pre-loaded with the centroids data of the current iteration
		// the mapper is passed with points data, match with the nearest centrod
		// map output: k=text of the centroid coordinate, v = point_x, point_y, 1.
		
		// The centroid file as distributed cache to all mappers, saved in "centroids"
		private ArrayList <double[]> centroids = new ArrayList <double[]> ();
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException, FileNotFoundException {
			// configure the distributed cache from the centroid file (localPaths[0])
			Path[] localPaths = context.getLocalCacheFiles(); 
			if (localPaths.length == 0) {throw new FileNotFoundException("Distributed cache file not found."); }
			
			File localFile = new File(localPaths[0].toUri());
		    BufferedReader points = new BufferedReader(new FileReader(localFile));
		    while (true) {
		    	String l = points.readLine();
                if (l!=null && !l.equals("")) {
		    		String[] p = l.split("[,\t]"); //p[0]=idx, p[1] = x, p[2]=y, l[3]=#points
		    		centroids.add(new double[]{Double.parseDouble(p[1]), Double.parseDouble(p[2])});
		    	}
                else {break;}
		    }
		}
		
		@Override
		public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
			// get each map 
			// tell if the point is in the window
			String[] p = value.toString().split(",");  // p[0]=point_x, p[1]=point_y
			double x = Double.parseDouble(p[0]);
			double y = Double.parseDouble(p[1]);
			//iterate the centroids ArrayList, find the nearest centroid
			double minDist = Double.MAX_VALUE; 
			// double[] center = new double[] {0.d,0.d};
			int center_idx = -1;
			int idx = 1;
			for(double[] c: centroids) {
				double d = Math.sqrt(Math.pow(c[0]-x,2)+Math.pow(c[1]-y, 2));
				if ( d < minDist) {
				    minDist = d;
				    center_idx = idx;
				}
				idx ++;
			}
			// k = centroid_index and v = "p_x,p_y,1", append "1" at end to indicate count of point
		    context.write(new IntWritable(center_idx), new Text(x + "," + y + ",1"));
			}
		}
    
    public static class kMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    	private ArrayList <double[]> centroids = new ArrayList <double[]> ();
    	
    	//use setup to load the previous centroids
    	@Override
		protected void setup(Context context)throws IOException, InterruptedException, FileNotFoundException {
			// configure the distributed cache from the centroid file (localPaths[0])
			Path[] localPaths = context.getLocalCacheFiles(); 
			if (localPaths.length == 0) {throw new FileNotFoundException("Distributed cache file not found."); }
			
			File localFile = new File(localPaths[0].toUri());
		    BufferedReader points = new BufferedReader(new FileReader(localFile));
		    while (true) {
		    	String l = points.readLine();
                if (l!=null && !l.equals("")) {
                	String[] p = l.split("[,\t]"); //p[0]=idx, p[1] = x, p[2]=y, p[3]=#points
		    		centroids.add(new double[]{Double.parseDouble(p[1]), Double.parseDouble(p[2])});
		    	}
                else {break;}
		    }
		}
    	
    	//use a counter in reducer to indicate if we need more loops
    	public enum Counter{
    		CONVERGED//name of the counter
    		//CLUSTER_idx //name of the counter counting 
    		}
    		
    	// To also enable combiner, the output should be in such format:
    	// key = centroid idx, value = intercenter_x, intercenter_y, #point_count
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			Iterator<Text> iter = values.iterator();
			// keep records of the new centers, then compare with the distributed "centroids" 
			// note sorted after mapping by the map key, so no need to sort again
			int countTotal = 0;
			double sumX = 0.d;
			double sumY = 0.d;
			
			while(iter.hasNext()) {
				Text t = new Text(iter.next());
				String[] s = t.toString().split(",");  // s[0]= c_x, s[1]=c_y, s[2]=#count
				if (!s[0].equals("")) {
					double count = Double.parseDouble(s[2]);
					sumX += Double.parseDouble(s[0]) * count;
					sumY += Double.parseDouble(s[1]) * count;
					countTotal += (int) count;
			    }
			}
			double c_x = sumX / countTotal;
			double c_y = sumY / countTotal;
			context.write(key, new Text(c_x + "," + c_y + "," + countTotal));
			
			// if same to the center in previous centroids ArrayList, counter ++
			double[] c_old = centroids.get(key.get()-1);
			if (c_old[0] == c_x && c_old[1] == c_y){
				// increase the counter "CONVERGED" by 1
				context.getCounter(Counter.CONVERGED).increment(1);  // counter = -1
		    }
		}
    }
    
    public static void main(String[] args) throws Exception,IOException,FileNotFoundException{
		// use a loop for multiple jobs
		if (args.length != 4) {
			throw new Exception("Usage: <pointFile> <#centroids> <centroidFile> <outputDir>");
		}
		// default max 6 iters
		int num_loops = 6;
		// default the output folder name as "/kmeans/iter_"+num_iter
		int iter = 1;
		String whereIsCentroid = args[2];
		
		int k = Integer.parseInt(args[1]);

	    Path pointInputPath = new Path(args[0]);
		while(iter <= num_loops) {
			Configuration conf = new Configuration();
			Job job = new Job(conf, "KMeans2"); 
			job.setJarByClass(KMeans2.class);
			// if not first loop, the previous centroid file should be:
			if (iter > 1) {whereIsCentroid = args[3]+"/iter_" + (iter - 1) + "/part-r-00000";} 
			Path centroidInput = new Path(whereIsCentroid);
			Path outputPath = new Path(args[3]+"/iter_" + iter); 
			//String thisCentroid = "/iter_" + iter + "/part-r-00000";
			 
			
			// set input/output directory
			FileInputFormat.addInputPath(job, pointInputPath);
			FileOutputFormat.setOutputPath(job, outputPath);
			
			//take the centroid file as cache file
			job.addCacheFile(centroidInput.toUri());
			
			//set mapper, reducer class
			job.setMapperClass(kMeansMapper.class);
			job.setReducerClass(kMeansReducer.class);
			job.setCombinerClass(kMeansReducer.class);
			
			// force single reducer
			job.setNumReduceTasks(1);
			
			//set K, V classes
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(IntWritable.class); 
			job.setOutputValueClass(Text.class);
			
			job.waitForCompletion(true);
			// if the new centers and previous centers are same
			Long counterValue = job.getCounters().findCounter(kMeansReducer.Counter.CONVERGED).getValue(); 
			
			if (counterValue == k) {break;}	// if converged, stop iteration
			else {iter++;}
		}
	}
}