import org.apache.commons.codec.digest.DigestUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
public class LoadLogs{


	public static Put get_put(String currentLine) throws ParseException{
		byte[] rowkey = DigestUtils.md5(currentLine);		
		Put p = new Put(rowkey);
		p.addColumn(Bytes.toBytes("raw"), Bytes.toBytes("line"), Bytes.toBytes(currentLine));
		Pattern pattern = Pattern.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$");
		Matcher matcher = pattern.matcher(currentLine);
		String hostname="";
		long date=0L;
		String URLpath="";
		long numberOfBytes=0L;
		SimpleDateFormat dateparse =new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
		
			if(matcher.find()){
				hostname=matcher.group(1);
				date = dateparse.parse(matcher.group(2)).getTime();
				//datetime=date.getTime();
				URLpath=matcher.group(3);
				numberOfBytes=Long.parseLong(matcher.group(4));




				p.addColumn(Bytes.toBytes("struct"), Bytes.toBytes("host"), Bytes.toBytes(hostname));
				p.addColumn(Bytes.toBytes("struct"), Bytes.toBytes("date"), Bytes.toBytes(date));
				p.addColumn(Bytes.toBytes("struct"), Bytes.toBytes("path"), Bytes.toBytes(URLpath));
				p.addColumn(Bytes.toBytes("struct"), Bytes.toBytes("bytes"), Bytes.toBytes(numberOfBytes));
			}

		
		return p;
	}







	public static void main(String[] args) throws Exception {
		String tableName= args[0];
		String filePath=args[1];
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf(tableName));
		BufferedReader br = null;


		//
		//System.out.println(date.getTime());


		try {

			String currentLine="";

			File fileDir = new File(filePath);

			br= new BufferedReader(
					new InputStreamReader(
							new FileInputStream(fileDir), "UTF8"));

			//br = new BufferedReader(new FileReader("/home/amit/Desktop/nasa-logs-1/NASA_access_log_Aug95-part.txt"));

			while ((currentLine = br.readLine()) != null) {
				Put p1= LoadLogs.get_put(currentLine);
				table.put(p1);

			}

			table.close();
			connection.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

	}



}
