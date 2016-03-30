import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class pageRank 
{

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
  {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String url_pr = new String();
    
    private String url = new String();
    private String pr = new String();
    private String one_outlink = new String();
    private String outlinks = new String();
    
    private double pr_sur_n;
    private int n_outlinks;
    private Text output = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
      StringTokenizer itr = new StringTokenizer(value.toString());
      StringTokenizer itr_glob = new StringTokenizer(value.toString(), "\t");
      StringTokenizer itr_url_pr = new StringTokenizer(value.toString(), ",");
      
      if(itr_glob.hasMoreTokens())
      {
	      url_pr = itr_glob.nextToken();
	      itr_url_pr = new StringTokenizer(url_pr, ",");
	      url = itr_url_pr.nextToken();
	      pr = itr_url_pr.nextToken();
	      
	      if(itr_glob.hasMoreTokens())
	      {
	    	outlinks = itr_glob.nextToken();
	    	//context.write(new Text(url), new Text("outlinks:" + outlinks));
	    	StringTokenizer itr_outlinks = new StringTokenizer(outlinks.toString(), ",");
	    	StringTokenizer itr_n_outlinks = new StringTokenizer(outlinks.toString(), ",");
	    	n_outlinks = 0;
	    	while(itr_n_outlinks.hasMoreTokens())
	    	{
	    		itr_n_outlinks.nextToken();
	    		n_outlinks += 1;
	    	}
	    	while(itr_outlinks.hasMoreTokens())
	    	{
	    		one_outlink = itr_outlinks.nextToken();
	    		pr_sur_n = Double.parseDouble(pr)/n_outlinks;
	    		context.write(new Text(one_outlink), new Text(Double.toString(pr_sur_n)));
	    	}
	    	
	      }
      }
    }
    
  }

  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> 
  {
    private IntWritable result = new IntWritable();
    private String nature = new String();
    private String content = new String();
    private String outlinks = new String();
    private double pr;
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
    {
      double sum = 0;
      for (Text val : values) 
      {
    	  sum += Double.parseDouble(val.toString());
      }
    	  context.write(key, new Text(Double.toString(sum)));
    }
  }

  public static void main(String[] args) throws Exception 
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(pageRank.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}