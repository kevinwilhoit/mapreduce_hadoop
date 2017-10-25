// Kevin Wilhoit
// 
// 
package prog1;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Median extends Configured implements Tool{
	
	public static class movie {
		public movie(Text text, Text text2, Vector<Text> vector) {
			movieid.set(text);
			title.set(text2);
			genres = vector;
		}
		public Text movieid = new Text();
		public Text title = new Text();
		public Vector<Text> genres = new Vector<Text>();
	}
	
	public static class genre_median {
		public genre_median(Text text, double rating2) {
			genre.set(text);
		}
		public Text genre = new Text();
		public Vector<Double> rating = new Vector<Double>();
		public int rating_counter = 0;
	}
	
	public static class median {
		public Text movieid = new Text();
		public Vector<Text> ratings = new Vector<Text>();
		public Text rating = new Text();
	}
	
	public static class Map_median extends Mapper<Object, Text, Text, DoubleWritable> {
	    private Text movieid = new Text();
	    private double rating = 0.0;

	    public void map(Object key, Text value, Context context) 
	    				throws IOException, InterruptedException {
	        String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line, "::");
	        tokenizer.nextToken();
	        movieid.set(tokenizer.nextToken());
	        rating = Double.parseDouble(tokenizer.nextToken());
	        context.write(movieid, new DoubleWritable(rating));
	    }
	 }
	
	 public static class Reduce_median extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
	      throws IOException, InterruptedException {
	        double sum = 0.0;
	        int i = 0;
	        Vector<Double> temp = new Vector<Double>();
	        for (DoubleWritable val : values) {
	            sum = val.get();
	            temp.add(sum);
	        }
	        String k = key.toString();
	        while(i < temp.size()){
	        	k += "::" + temp.elementAt(i);
	        	i++;
	        }
	        key.set(k);
	        context.write(key, new DoubleWritable(sum));
	        
	    }
	 }

	 public int run(String[] args) throws Exception{
		 	Vector<movie> movies = new Vector<movie>();
			Vector<median> movie_medians = new Vector<median>();
			
			Vector<genre_median> genrelist = new Vector<genre_median>();
			genrelist.add(new genre_median(new Text("Action"), 0.0));genrelist.add(new genre_median(new Text("Adventure"), 0.0));genrelist.add(new genre_median(new Text("Animation"), 0.0));genrelist.add(new genre_median(new Text("Children"), 0.0));genrelist.add(new genre_median(new Text("Comedy"), 0.0));genrelist.add(new genre_median(new Text("Crime"), 0.0));genrelist.add(new genre_median(new Text("Documentary"), 0.0));genrelist.add(new genre_median(new Text("Drama"), 0.0));genrelist.add(new genre_median(new Text("Fantasy"), 0.0));genrelist.add(new genre_median(new Text("Film-Noir"), 0.0));genrelist.add(new genre_median(new Text("Horror"), 0.0));genrelist.add(new genre_median(new Text("Musical"), 0.0));genrelist.add(new genre_median(new Text("Mystery"), 0.0));genrelist.add(new genre_median(new Text("Romance"), 0.0));genrelist.add(new genre_median(new Text("Sci-Fi"), 0.0));genrelist.add(new genre_median(new Text("Thriller"), 0.0));genrelist.add(new genre_median(new Text("War"), 0.0));genrelist.add(new genre_median(new Text("Western"), 0.0));
			
			try{
				BufferedReader reader = new BufferedReader(new FileReader("movies.dat"));
				String line = null;
				while ((line = reader.readLine()) != null) {
					movie temp = new movie(new Text(""), new Text(""), new Vector<Text>());
					StringTokenizer tokenizer1 = new StringTokenizer(line, "::");
					temp.movieid.set(tokenizer1.nextToken());
					temp.title.set(tokenizer1.nextToken());
					StringTokenizer tokenizer2 = new StringTokenizer(tokenizer1.nextToken(), "|");
					while(tokenizer2.hasMoreTokens()) {
						temp.genres.add(new Text(tokenizer2.nextToken()));
					}
					movies.add(temp);
				}
				reader.close();
			}catch(Exception e){
				System.out.println("Error while reading file line by line: " + e.getMessage());
			}
			
		    Configuration conf = new Configuration();

		    Job job = new Job(conf, "median");
		    job.setJarByClass(Median.class);

		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(DoubleWritable.class);

		    job.setMapperClass(Map_median.class);
		    job.setReducerClass(Reduce_median.class);

		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);

		    FileInputFormat.addInputPath(job, new Path("x000"));
		    FileInputFormat.addInputPath(job, new Path("x001"));
		    FileInputFormat.addInputPath(job, new Path("x002"));
		    FileInputFormat.addInputPath(job, new Path("x003"));
		    FileInputFormat.addInputPath(job, new Path("x004"));
		    FileInputFormat.addInputPath(job, new Path("x005"));
		    FileInputFormat.addInputPath(job, new Path("x006"));
		    FileInputFormat.addInputPath(job, new Path("x007"));
		    FileInputFormat.addInputPath(job, new Path("x008"));
		    FileInputFormat.addInputPath(job, new Path("x009"));
		    FileInputFormat.addInputPath(job, new Path("x010"));
		    FileOutputFormat.setOutputPath(job, new Path("output"));

		    job.waitForCompletion(true);
		    
		    try{
				BufferedReader reader2 = new BufferedReader(new FileReader("output/part-r-00000"));
				String line2 = null;
				while ((line2 = reader2.readLine()) != null) {
					median temp = new median();
					StringTokenizer tokenizer1 = new StringTokenizer(line2, "	");
					StringTokenizer tokenizer2 = new StringTokenizer(tokenizer1.nextToken(), "::");
					temp.movieid.set(tokenizer2.nextToken());
					while(tokenizer2.hasMoreTokens()) {
						temp.ratings.add(new Text(tokenizer2.nextToken()));
					}
					temp.rating.set(tokenizer1.nextToken());
					movie_medians.add(temp);
				}
				reader2.close();
			}catch(Exception e){
				System.out.println("Error while reading file line by line: " + e.getMessage());
			}
		    
		    for(int i = 0; i < movie_medians.size(); i++){
		    	int j = Integer.parseInt( movie_medians.elementAt(i).movieid.toString() );
	    		int temp1 = 0;
	    		int movies_index = 0;
		    	for(int k = 0; k < movies.size(); k++){
		    		int temp_t = Integer.parseInt( movies.elementAt(k).movieid.toString() );
		    		if(temp_t == j) {
		    			temp1 = temp_t;
		    			movies_index = k;
		    			break;
		    		}
		    	}
	    		if(temp1 == j){
	    			for(int k = 0; k < movies.elementAt(movies_index).genres.size(); k++){
	    				for (int l = 0; l < genrelist.size(); l++){
	    					if(genrelist.elementAt(l).genre.toString().equals( movies.elementAt(movies_index).genres.elementAt(k).toString() ) ) {
	    						int p = 0;
	    						while( p < movie_medians.elementAt(i).ratings.size() ){
	    							genrelist.elementAt(l).rating.add( Double.parseDouble( movie_medians.elementAt(i).ratings.elementAt(p).toString() ) );
	    							p++;
	    						}
	    						break;
	    					}
	    				}
	    			}
	    		}
		    }
		    
		    System.out.println("Output for median calculation on full dataset:");
		    for(int i = 0; i < genrelist.size(); i++) {
		    	double temp = 0.0;
		    	Collections.sort(genrelist.elementAt(i).rating);
		    	
		        if(genrelist.elementAt(i).rating.size() == 1) temp = genrelist.elementAt(i).rating.elementAt(0);
		        else if(genrelist.elementAt(i).rating.size() % 2 == 1) temp = genrelist.elementAt(i).rating.elementAt(genrelist.elementAt(i).rating.size() / 2 + 1);
		        else {
		        	temp += genrelist.elementAt(i).rating.elementAt(genrelist.elementAt(i).rating.size() / 2);
		        	temp += genrelist.elementAt(i).rating.elementAt((genrelist.elementAt(i).rating.size() / 2) + 1);
		        	temp /= 2;
		        }
		        
		    	if(i == 1 || i == 2 || i == 3 || i == 6 || i == 9 || i == 15) System.out.println(genrelist.elementAt(i).genre.toString() + "	" + temp);
		    	else System.out.println(genrelist.elementAt(i).genre.toString() + "		" + temp);
		    }
		    
		    return 0;
	 }
	 
	 public static void main(String[] args) throws Exception {
		try{
			File file =  new File("output");
			FileUtils.deleteDirectory(file);
			System.out.println("Folder: " + file.getName() + " is deleted!");
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("Error: Folder was not deleted.");
		}
		
		int ret = ToolRunner.run(new Median(), args);
		System.exit(ret);
	    
	 }
	 
}
