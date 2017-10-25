// Kevin Wilhoit
// 
// 
package prog1;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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

public class Mean extends Configured implements Tool{
	
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
	
	public static class genre_mean {
		public genre_mean(Text text, double rating2) {
			genre.set(text);
			rating = rating2;
		}
		public Text genre = new Text();
		public double rating = 0.0;
		public int rating_counter = 0;
	}
	
	public static class mean {
		public Text movieid = new Text();
		public Text rating = new Text();
		public Text rating_count = new Text();
	}
	
	public static class Map_mean extends Mapper<Object, Text, Text, DoubleWritable> {
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
	
	 public static class Reduce_mean extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
	      throws IOException, InterruptedException {
	        double sum = 0.0;
	        int i = 0;
	        for (DoubleWritable val : values) {
	            sum += val.get();
	            i++;
	        }
	        String k = key.toString();
	        k = k + "::" + i;
	        key.set(k);
	        context.write(key, new DoubleWritable(sum));
	        
	    }
	 }

	 public int run(String[] args) throws Exception{
		 	Vector<movie> movies = new Vector<movie>();
			Vector<mean> movie_means = new Vector<mean>();
			
			Vector<genre_mean> genrelist = new Vector<genre_mean>();
			genrelist.add(new genre_mean(new Text("Action"), 0.0));genrelist.add(new genre_mean(new Text("Adventure"), 0.0));genrelist.add(new genre_mean(new Text("Animation"), 0.0));genrelist.add(new genre_mean(new Text("Children"), 0.0));genrelist.add(new genre_mean(new Text("Comedy"), 0.0));genrelist.add(new genre_mean(new Text("Crime"), 0.0));genrelist.add(new genre_mean(new Text("Documentary"), 0.0));genrelist.add(new genre_mean(new Text("Drama"), 0.0));genrelist.add(new genre_mean(new Text("Fantasy"), 0.0));genrelist.add(new genre_mean(new Text("Film-Noir"), 0.0));genrelist.add(new genre_mean(new Text("Horror"), 0.0));genrelist.add(new genre_mean(new Text("Musical"), 0.0));genrelist.add(new genre_mean(new Text("Mystery"), 0.0));genrelist.add(new genre_mean(new Text("Romance"), 0.0));genrelist.add(new genre_mean(new Text("Sci-Fi"), 0.0));genrelist.add(new genre_mean(new Text("Thriller"), 0.0));genrelist.add(new genre_mean(new Text("War"), 0.0));genrelist.add(new genre_mean(new Text("Western"), 0.0));
			
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

		    Job job = new Job(conf, "mean");
		    job.setJarByClass(Mean.class);

		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(DoubleWritable.class);

		    job.setMapperClass(Map_mean.class);
		    job.setReducerClass(Reduce_mean.class);

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
					mean temp = new mean();
					StringTokenizer tokenizer1 = new StringTokenizer(line2, "	");
					StringTokenizer tokenizer2 = new StringTokenizer(tokenizer1.nextToken(), "::");
					temp.movieid.set(tokenizer2.nextToken());
					temp.rating_count.set(tokenizer2.nextToken());
					temp.rating.set(tokenizer1.nextToken());
					movie_means.add(temp);
				}
				reader2.close();
			}catch(Exception e){
				System.out.println("Error while reading file line by line: " + e.getMessage());
			}
		    
		    for(int i = 0; i < movie_means.size(); i++){
		    	int j = Integer.parseInt( movie_means.elementAt(i).movieid.toString() );
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
	    						genrelist.elementAt(l).rating += Double.parseDouble( movie_means.elementAt(i).rating.toString() );
	    						genrelist.elementAt(l).rating_counter += Integer.parseInt(movie_means.elementAt(i).rating_count.toString());
	    						break;
	    					}
	    				}
	    			}
	    		}
		    }
		    
		    System.out.println("Output for mean calculation on full dataset:");
		    for(int i = 0; i < genrelist.size(); i++) {
		    	genrelist.elementAt(i).rating = genrelist.elementAt(i).rating / (genrelist.elementAt(i).rating_counter - 1.0);
		    	if(i == 1 || i == 2 || i == 3 || i == 6 || i == 9 || i == 15) System.out.println(genrelist.elementAt(i).genre.toString() + "	" + genrelist.elementAt(i).rating);
		    	else System.out.println(genrelist.elementAt(i).genre.toString() + "		" + genrelist.elementAt(i).rating);
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
			System.out.println("Error: Folder was not delete.");
		}
		
		int ret = ToolRunner.run(new Mean(), args);
		System.exit(ret);
	    
	 }
	 
}