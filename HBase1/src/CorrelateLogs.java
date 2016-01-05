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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
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

public class CorrelateLogs extends Configured implements Tool {
	
	public static class MyMapper
    extends TableMapper<Text, LongWritable>{
 

    
        
        
        @Override
        public void map(ImmutableBytesWritable row, Result values, Context context
                ) throws IOException, InterruptedException {
  	 	  	
        		//String id = Bytes.toString(row.get());
    			
        		byte[] host = values.getValue(Bytes.toBytes("struct"), Bytes.toBytes("host"));
        		byte[] by =  values.getValue(Bytes.toBytes("struct"), Bytes.toBytes("bytes"));
        		String hostName="";long bytes=0;
        		if (host!= null && by != null){
        			
        			hostName = Bytes.toString(host);
        			bytes=Bytes.toLong(by);
        			context.write(new Text(hostName),new LongWritable(bytes));
        		}
    		
        	
        	
        	
            }
        	
        	
        	
        	
        	
        	}
	public static class MyReducer
    extends Reducer<Text, LongWritable, LongWritable, LongWritable> {
        
		private LongWritable result = new LongWritable();
        
        @Override
        public void reduce(Text key, Iterable<LongWritable> values,
                Context context
                ) throws IOException, InterruptedException {
        	long xcount=0; long ybytes=0;
        	
        	for (LongWritable val : values) {
        		ybytes+=val.get();
        		++xcount;
            }
        	result.set(ybytes);
            context.write(new LongWritable(xcount), result); 
        }
    }
	
	public static class MyMapper2
    extends Mapper<LongWritable, LongWritable, Text, DoubleWritable>{
       
		long noOfDataPoints;
		long xsum;
		long ysum;
		long xsqrsum;
		long ysqrsum;
		long xyprodsum;
		
		@Override
		protected void setup(Context context)
	              throws java.io.IOException,
	                     java.lang.InterruptedException{
			noOfDataPoints=0L;
			xsum=0L;
			ysum=0L;
			xsqrsum=0L;
			ysqrsum=0L;
			xyprodsum=0L;
		}
    
        
        
        @Override
        public void map(LongWritable x, LongWritable y, Context context
                ) throws IOException, InterruptedException {
        	long x1 = x.get(); long y1= y.get();
        	xsum += x1;
        	ysum += y1;
        	xsqrsum += (x1*x1);
        	ysqrsum += (y1*y1);
        	xyprodsum += (x1*y1);
        	++noOfDataPoints;
            }
        @Override
        protected void cleanup(Context context)
                throws java.io.IOException,
                       java.lang.InterruptedException{
        	 double r = 0.0d;
        	 double noOfDataPoints1=noOfDataPoints;
        	 double xyprodsum1= xyprodsum;
        	 double xsum1=xsum;
        	 double xsqrsum1=xsqrsum;
        	 double ysqrsum1=ysqrsum;
        	 double ysum1=ysum;
        	double numr= (noOfDataPoints1*xyprodsum1)-(xsum1*ysum1);
        			
        	double denomr = Math.sqrt((noOfDataPoints1*xsqrsum1)-(xsum1*xsum1)) * Math.sqrt((noOfDataPoints1*ysqrsum1)-(ysum1*ysum1)) ;
        	r= numr/denomr ;
        	System.out.println("No of Data Points \t"+noOfDataPoints);
        	System.out.println("xsum \t"+xsum);
        	System.out.println("xsqrsum \t"+xsqrsum);
        	System.out.println("ysum \t"+ysum);
        	System.out.println("ysqrsum \t"+ysqrsum);
        	System.out.println("xyprodsum \t"+xyprodsum);
        	System.out.println("r \t"+r);
        	System.out.println("rsq \t"+r*r);
        	
        	context.write(new Text("No of Data Points "),new DoubleWritable(noOfDataPoints));
        	context.write(new Text("xsum"),new DoubleWritable(xsum));
        	context.write(new Text("xsqrsum"),new DoubleWritable(xsqrsum));
        	context.write(new Text("ysum"),new DoubleWritable(ysum));
        	context.write(new Text("ysqrsum"),new DoubleWritable(ysqrsum));
        	context.write(new Text("xyprodsum"),new DoubleWritable(xyprodsum));
        	context.write(new Text("r"),new DoubleWritable(r));
        	context.write(new Text("rsq"),new DoubleWritable(r*r));
        	//context.write(new Text("numerator"),new DoubleWritable(numr));
        	
        }
        
	}
	
	 public static void main(String[] args) throws Exception {
	        int res = ToolRunner.run(new Configuration(), new CorrelateLogs(), args);
	        System.exit(res);
	    }
	 
	    @Override
	    public int run(String[] args) throws Exception {
	        Configuration conf = this.getConf();
	        Job job = Job.getInstance(conf, "Correlate Logs");
	        job.setJarByClass(CorrelateLogs.class);
	        job.setInputFormatClass(TextInputFormat.class);
	        Scan scan = new Scan();
	     //   scan.setCaching(500);
	   //     scan.setCacheBlocks(false);
	       
            // Configure the Map process to use HBase
	        TableMapReduceUtil.addDependencyJars(job);
            TableMapReduceUtil.initTableMapperJob(args[0],scan,MyMapper.class,Text.class,LongWritable.class,job );                         
            job.setNumReduceTasks(1);
	        
	 	     job.setMapperClass(MyMapper.class);
	        //job.setCombinerClass(MyReducer.class);
	        job.setReducerClass(MyReducer.class);
	                
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(LongWritable.class);
	        
	        job.setOutputKeyClass(LongWritable.class);
	        job.setOutputValueClass(LongWritable.class);
	        job.setOutputFormatClass(TextOutputFormat.class);
	        //TextInputFormat.addInputPath(job, new Path(args[0]));
	        TextOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	        ChainReducer.setReducer(job, MyReducer.class, Text.class, LongWritable.class,
	        		LongWritable.class, LongWritable.class, new Configuration(false));
	        ChainReducer.addMapper(job, MyMapper2.class, LongWritable.class, LongWritable.class,
	        		   LongWritable.class, DoubleWritable.class, new Configuration(false));
	        
	        return job.waitForCompletion(true) ? 0 : 1;
	    }

}
