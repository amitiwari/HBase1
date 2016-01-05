import org.apache.commons.codec.digest.DigestUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
public class LoadLogsMR extends Configured implements Tool {
	
	/*public static class TokenizerMapper
    extends Mapper<LongWritable, Text, LongWritable, Text >{
 

    
        
        
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
  	 	  	
        	
        	context.write(key,value);
        	
            }
        	
        	
        	
        	
        	
        	}*/
        
	public static class MyReducer
    extends TableReducer<LongWritable, Text, LongWritable> {
       
        @Override
        public void reduce(LongWritable key, Iterable<Text>
        value, Context context
	                ) throws IOException, InterruptedException {
        	Put putobj=null;
        	for (Text val : value) {
        	String currentLine = val.toString();
        	
        	//LoadLogs obj=new LoadLogs();
			
			try {
				putobj = LoadLogs.get_put(currentLine);
	        	context.write(key, putobj); 	
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	}
			
        }
				
	}	
	
        
    
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LoadLogsMR(), args);
        System.exit(res);
    }
 
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "LoadLogsMR");
        job.setJarByClass(LoadLogsMR.class);
 
        job.setInputFormatClass(TextInputFormat.class);
 
       // job.setMapperClass(TokenizerMapper.class);
       // job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
 
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.initTableReducerJob(args[1], MyReducer.class, job);
        job.setNumReduceTasks(3);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        /*job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class)*/;
        TextInputFormat.addInputPath(job, new Path(args[0]));
       // TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
	



}
