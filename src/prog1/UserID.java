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

public class UserID extends Configured implements Tool{
	
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
	
	public static class genre_userid {
		public genre_userid(Text text, double rating2) {
			genre.set(text);
		}
		public Text genre = new Text();
		public double rating = 0.0;
		public int rating_counter = 0;
	}
	
	public static class userid {
		public Text userid = new Text();
		public Vector<Text> movies = new Vector<Text>();
	}
	
	public static class Map_user extends Mapper<Object, Text, Text, Text> {
		private Text userid = new Text();
	    private Text movieid = new Text();

	    public void map(Object key, Text value, Context context) 
	    				throws IOException, InterruptedException {
	        String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line, "::");
	        userid.set(tokenizer.nextToken());
	        movieid.set(tokenizer.nextToken());
	        context.write(userid, movieid);
	    }
	 }
	
	 public static class Reduce_user extends Reducer<Text, Text, Text, Text> {
	    public void reduce(Text key, Iterable<Text> values, Context context)
	      throws IOException, InterruptedException {
	        Text movies = new Text();
	        String m = new String();
	        String k = key.toString();
	        int i = 0;
	        for (Text val : values) {
	        	if(i == 0) m = val.toString();
	        	else m = m + "::" + val.toString();
	            i++;
	        }
	        key.set(k);
	        movies.set(m);
	        context.write(key, movies);
	    }
	 }

	 public int run(String[] args) throws Exception{
		 	Vector<movie> movies = new Vector<movie>();
			Vector<userid> movie_userids = new Vector<userid>();
			Vector<Integer> genres = new Vector<Integer>();
			
			genres.add(0);genres.add(0);genres.add(0);genres.add(0);genres.add(0);genres.add(0);genres.add(0);genres.add(0);genres.add(0);genres.add(0);genres.add(0);genres.add(0);genres.add(0);genres.add(0);genres.add(0);genres.add(0);genres.add(0);genres.add(0);
			
			
			Vector<genre_userid> genrelist = new Vector<genre_userid>();
			genrelist.add(new genre_userid(new Text("Action"), 0.0));genrelist.add(new genre_userid(new Text("Adventure"), 0.0));genrelist.add(new genre_userid(new Text("Animation"), 0.0));genrelist.add(new genre_userid(new Text("Children"), 0.0));genrelist.add(new genre_userid(new Text("Comedy"), 0.0));genrelist.add(new genre_userid(new Text("Crime"), 0.0));genrelist.add(new genre_userid(new Text("Documentary"), 0.0));genrelist.add(new genre_userid(new Text("Drama"), 0.0));genrelist.add(new genre_userid(new Text("Fantasy"), 0.0));genrelist.add(new genre_userid(new Text("Film-Noir"), 0.0));genrelist.add(new genre_userid(new Text("Horror"), 0.0));genrelist.add(new genre_userid(new Text("Musical"), 0.0));genrelist.add(new genre_userid(new Text("Mystery"), 0.0));genrelist.add(new genre_userid(new Text("Romance"), 0.0));genrelist.add(new genre_userid(new Text("Sci-Fi"), 0.0));genrelist.add(new genre_userid(new Text("Thriller"), 0.0));genrelist.add(new genre_userid(new Text("War"), 0.0));genrelist.add(new genre_userid(new Text("Western"), 0.0));
			
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
		    job.setJarByClass(UserID.class);

		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);

		    job.setMapperClass(Map_user.class);
		    job.setReducerClass(Reduce_user.class);

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
					userid temp = new userid();
					StringTokenizer tokenizer1 = new StringTokenizer(line2, "	");
					temp.userid.set(tokenizer1.nextToken());
					StringTokenizer tokenizer2 = new StringTokenizer(tokenizer1.nextToken(), "::");
					while(tokenizer2.hasMoreTokens()) {
						temp.movies.add(new Text(tokenizer2.nextToken()));
					}
					movie_userids.add(temp);
				}
				reader2.close();
			}catch(Exception e){
				System.out.println("Error while reading file line by line: " + e.getMessage());
			}
		    
		    int user_index = 0;
		    for(int i = 0; i < movie_userids.size(); i++){
		    	if(movie_userids.elementAt(i).movies.size() > movie_userids.elementAt(user_index).movies.size()) user_index = i;
		    }
		    
		    for(int i = 0; i < movie_userids.elementAt(user_index).movies.size(); i++){
		    	int p = Integer.parseInt( movie_userids.elementAt(user_index).movies.elementAt(i).toString() );
	    		int temp1 = 0;
	    		int movies_index = 0;
		    	for(int k = 0; k < movies.size(); k++){
		    		int temp_t = Integer.parseInt( movies.elementAt(k).movieid.toString() );
		    		if(temp_t == p) {
		    			temp1 = temp_t;
		    			movies_index = k;
		    			break;
		    		}
		    	}
		    	if(temp1 == p){
	    			for(int k = 0; k < movies.elementAt(movies_index).genres.size(); k++){
	    				for (int l = 0; l < genrelist.size(); l++){
	    					if(genrelist.elementAt(l).genre.toString().equals( movies.elementAt(movies_index).genres.elementAt(k).toString() ) ) {
	    						genres.set(l, genres.elementAt(l) + 1);
	    						break;
	    					}
	    				}
	    			}
	    		}
		    }
		    
		    int genre_index = 0;
		    for(int i = 0; i < genres.size(); i++){
		    	if(genres.elementAt(i) > genres.elementAt(genre_index)) genre_index = i;
		    }
		    
		    System.out.println("User Identification");
		    System.out.println(movie_userids.elementAt(user_index).userid.toString() + " -- Total Rating Counts: " + movie_userids.elementAt(user_index).movies.size() + " -- Most Rated Genre: " + genrelist.elementAt(genre_index).genre.toString() + " - " + genres.elementAt(genre_index));
		    
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
		
		int ret = ToolRunner.run(new UserID(), args);
		System.exit(ret);
	    
	 }
	 
}