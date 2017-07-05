/**
 * @author FengL
 */
package com.migu.bigdata.mapreduce;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;
import com.migu.bigdata.bean.RowNumEnum;

public class LzoFileFormat {
	static Logger log = Logger.getLogger(LzoFileFormat.class);

	// private static String fileStartWith="";
	public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable> {
		private MultipleOutputs<Text, NullWritable> mos;
		private Counter rowCounter;
		private String filestartwith = "";
		private String dilimer = "";
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			mos = new MultipleOutputs<Text, NullWritable>(context);
			filestartwith = context.getConfiguration().get("filestartwith");
			dilimer = context.getConfiguration().get("dilimer");
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().replace(dilimer, "").replace("€", dilimer);
			if (line.trim().length() > 1){
				rowCounter = context.getCounter(RowNumEnum.ROWCOUNTER);
				rowCounter.increment(1);
				mos.write(filestartwith, line, NullWritable.get());
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}
	
//    public static class MyReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
//        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{
//            context.write(key, NullWritable.get());
//        }
//    }

}
