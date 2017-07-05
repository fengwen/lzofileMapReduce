/**
 * @author FengL
 */
package com.migu.bigdata.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.hadoop.compression.lzo.LzopCodec;

/**
 * @author FengL
 * 从lzo文件中读取数据,即lzo解压缩 
 * @param lzoFilePath 
 * @param conf 
 * @return void 
 */
public class LzoFileUtils {
    private static Configuration getDefaultConf(){  
        Configuration conf = new Configuration();  
        conf.set("mapred.job.tracker", "local");  
        //conf.set("fs.default.name", "file:///");  
        conf.set("io.compression.codecs", "com.hadoop.compression.lzo.LzoCodec");  
        return conf;
    }
	 public static List<String> readLzoFile(Path lzoFilePath,FileSystem fs){
		Configuration conf= getDefaultConf();
		LzopCodec lzo=null;
		InputStream is=null;  
		InputStreamReader isr=null;  
		BufferedReader reader=null;  
		List<String> result=null;  
		String line=null;
		
	    try {  
	        lzo=new LzopCodec();  
	        lzo.setConf(conf);  
	        //is=lzo.createInputStream(new FileInputStream(lzoFilePath));  
			FSDataInputStream fsis = fs.open(lzoFilePath);	       
			is=lzo.createInputStream(fsis);
	        isr=new InputStreamReader(is);
	        reader=new BufferedReader(isr);  
	        result=new ArrayList<String>();  
	        while((line=reader.readLine())!=null){  
	          result.add(line);  
	        }  
	          
	      } catch (FileNotFoundException e) {  
	        // TODO Auto-generated catch block  
	        e.printStackTrace();  
	      } catch (IOException e) {  
	        // TODO Auto-generated catch block  
	        e.printStackTrace();  
	      }finally{  
	        try {  
	          if(reader!=null){  
	            reader.close();  
	          }  
	          if(isr!=null){  
	            isr.close();  
	          }  
	          if(is!=null){  
	            is.close();  
	          }  
	        } catch (IOException e) {  
	          // TODO Auto-generated catch block  
	          e.printStackTrace();  
	        }  
	      }  
	      return result;
	 }
}
