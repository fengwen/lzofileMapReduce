package com.migu.bigdata.mapreduce;

import java.util.HashMap;
import java.util.Map;

//import org.json.*;
public class HelloWorld {
	static void setUpEnvironment(String key,String value) {
		 ProcessBuilder builder=new ProcessBuilder();
		 Map<String, String> env = builder.environment();
		 env.put("Path", "");
		}

	public static void main(String[] args) {
//        JSONObject obj=new JSONObject();  
//        obj.put("name","foo");  
//        obj.put("num",new Integer(100));  
//        obj.put("balance",new Double(1000.21));  
//        obj.put("is_vip",new Boolean(true));  
//        obj.put("nickname","");  
       System.out.print(args.length);  
		//System.getenv("MAPNUM")
       
		Map<String,String> map=new HashMap<String,String>();
		map.put("datadir","dsfdfdsf");
		map.put("verifynum", "dfd");
		map.put("mapnum", "dfsdf");
		
		 System.out.print(map.toString()+"\n");  
		 setUpEnvironment("lzofile",map.toString());
		 System.out.print(System.getenv("lzofile"));  
	}

}
