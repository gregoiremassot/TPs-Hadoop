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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class WordCount 
{

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
  {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    /*String fileName = new String();
    protected void setup(Context context) throws java.io.IOException, java.lang.InterruptedException
    {
       fileName = ((FileSplit) context.getInputSplit()).getPath().toString();
    }*/
    
	//public Text fN = new Text(fileName);
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
    	FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        
    	//System.out.println(fN.toString());
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) 
      {
        word.set(itr.nextToken());
        context.write(word, new Text(fileName));
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> 
  {
    private Text result = new Text();
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
    {
     Text sum = new Text();
     for(Text val : values)
     {
    	 sum = new Text(sum.toString() + " " + val.toString());	
     }
     context.write(key,sum);
      //result.set(sum);
     //System.out.println(sum.toString());
      
    }
  }

  public static void main(String[] args) throws Exception 
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
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
