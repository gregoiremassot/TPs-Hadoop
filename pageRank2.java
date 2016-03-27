import java.io.IOException;
import java.util.StringTokenizer;
import java.util.LinkedList;
import java.util.ArrayList;

import java.util.ListIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class pageRank2 
{

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

    private final static IntWritable one = new IntWritable(1);
    private Text line = new Text();
    private Text url = new Text();
    private Text pageRank = new Text();
    private Text pR = new Text();
    private Text links = new Text();
    private Text links2 = new Text();
    //ArrayList<Text> l = new ArrayList<Text>();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
      while (itr.hasMoreTokens()) 
      {
        line.set(itr.nextToken());
        StringTokenizer itr2 = new StringTokenizer(line.toString(), ",");
        url.set(itr2.nextToken());
        pageRank.set(itr2.nextToken());
        StringTokenizer itr3 = new StringTokenizer(pageRank.toString(), "\t");
        pR.set(itr3.nextToken());
        if(itr3.hasMoreTokens())
        {
        	links.set(itr3.nextToken());
        	//l.add(links);
        	context.write(url, links);
        	while(itr2.hasMoreTokens())
        	{
        		
        		links2.set(itr2.nextToken());
        		context.write(url, links2);
        		//l.add(links2);
        	}
        }
        
        /*StringTokenizer itr4 = new StringTokenizer(links.toString(), ",");*/
        
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      /*for (IntWritable val : values) {
        sum += val.get();
      }*/
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(pageRank2.class);
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