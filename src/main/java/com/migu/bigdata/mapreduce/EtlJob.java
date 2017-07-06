/**
 * lzo文件格式化输出
 * @author FengL
 */
package com.migu.bigdata.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.hadoop.mapreduce.LzoTextInputFormat;
import com.migu.bigdata.bean.CheckFileBean;
import com.migu.bigdata.bean.ComparatorCheckFileBean;
import com.migu.bigdata.bean.RowNumEnum;
import com.migu.bigdata.mapreduce.LzoFileFormat.MapClass;
import com.migu.bigdata.util.HdfsUitls;
import com.migu.bigdata.util.LzoFileUtils;

public class EtlJob extends Configured implements Tool {
	private static Logger log = Logger.getLogger(EtlJob.class);
	private static Long allRecordNum = 0L;
	private static String inPath = "";
	private static String outPath = "";
	private static String startStr = "";
	private static String suffStr = "";
	private static String interType = "";
	private static String dilimer = "|";
	private static String filestartwith = "";

	/**
	 * Main entry point for the etl MapReduce Job.
	 *
	 * @param args
	 *            arguments
	 * @throws Exception
	 *             when something goes wrong
	 */
	public static void main(String[] args) throws Exception {
		int res = 0;
		log.info("---------------参数个数为：" + args.length);
		if (args.length != 5) {
			System.out.println("Usage:" + "\n		参数1：输入文件路径" + "\n		参数2：输出文件路径"
					+ "\n		参数3：文件名前缀。如：USERORDER_OTHER_DAY_2017070100.0000.dat.lzo.则输入为 USERORDER_OTHER_DAY.大小写敏感"
					+ "\n		参数4：lzo 固定输入参数"
					+ "\n		参数5：ALL|ADD|DELMOD。其中任意一个，忽略大小写.ALL表示一个文件下很多文件只处理最新的一个文件,其他全部处理" + "\nExample:"
					+ "\n		hadoop jar /data/fengwen/etl4lzofile-0.0.1-SNAPSHOT.jar "
					+ "/data/source/direct_data/migu-game/migu-game-mdsp-message/20170701/ "
					+ "/data/source/direct_data/migu-game/migu-game-mdsp-message-done/USER_THIRD_FEE_LOG_DAY/20170701 "
					+ "USER_THIRD_FEE_LOG_DAY lzo ADD"
					+ "\n			输出文件查看：hadoop dfs -ls /fengwen/out/out99.文件为竖线'|'分隔符号"
					+ "\n         执行结果0表示成功其他表示失败" + "\n			");
			res = -1;

		} else {

			inPath = args[0];
			outPath = args[1];
			startStr = args[2];
			suffStr = args[3].toLowerCase();
			interType = args[4].toLowerCase();

			log.info("inPath:" + inPath);
			log.info("outPath:" + inPath);
			log.info("startStr:" + startStr);
			log.info("suffStr:" + suffStr);
			log.info("interType:" + interType);
			res = ToolRunner.run(new Configuration(), new EtlJob(), args);
		}

		System.exit(res);
	}

	/**
	 * 获取校验文件列表
	 * 
	 * @param fs
	 * @return List<CheckFileBean>
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@SuppressWarnings("static-access")
	private List<CheckFileBean> getVerifyFile(Configuration conf) {
		// 获取校验文件列表
		FileSystem fs = null;
		List<CheckFileBean> lcheckFile = null;
		try {
			lcheckFile = new ArrayList<CheckFileBean>();
			Path in = new Path(inPath);
			fs = in.getFileSystem(conf);
			FileStatus[] status = fs.listStatus(in);
			for (FileStatus file : status) {
				if (file.isFile()) {
					String fileName = file.getPath().getName();
					if ((fileName.startsWith("VERIFY_" + startStr) || fileName.startsWith("VERIFY" + startStr))
							&& fileName.endsWith(suffStr)) {
						List<String> ls = LzoFileUtils.readLzoFile(file.getPath(), fs);
						for (String l : ls) {
							if (l.trim().length() == 0)
								continue;
							CheckFileBean chkFile = new CheckFileBean(l);
							// log.info(chkFile.printRows());
							lcheckFile.add(chkFile);
						}
					}
				}
			}
		} catch (Exception e) {
			lcheckFile = null;
			e.printStackTrace();
		} finally {
			if (fs != null) {
				try {
					fs.closeAll();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return lcheckFile;
	}

	/**
	 * 拼接需要进行通过Map处理的文件
	 * 
	 * @param lcheckFile
	 * @return
	 */
	private String getInputMapFileInfo(List<CheckFileBean> lcheckFile) {
		StringBuilder sb = new StringBuilder();
		if (interType.toUpperCase().equals("ALL")) {
			Comparator<CheckFileBean> cmp = new ComparatorCheckFileBean();
			Collections.sort(lcheckFile, cmp);
			if (lcheckFile.size() > 0) {
				int index = lcheckFile.size() - 1;
				CheckFileBean cf = lcheckFile.get(index);
				allRecordNum = cf.getRecordRows();
				String lzoDataFileName = inPath + "/" + cf.getFileName() + "." + suffStr;
				sb.append(lzoDataFileName).append(",");
			}
		} else {
			for (CheckFileBean cf : lcheckFile) {
				String lzoDataFileName = inPath + "/" + cf.getFileName() + "." + suffStr;
				sb.append(lzoDataFileName).append(",");
				allRecordNum = allRecordNum + cf.getRecordRows();
			}
		}
		if (sb.toString().trim().length() == 0) {
			return "";
		}
		return sb.toString().substring(0, sb.toString().length() - 1);
	}

	/**
	 * 创建输出目录，考虑到有较多的临时文件，因此先创建临时文件夹
	 * 
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("static-access")
	public Path createTmpOutDir(Configuration conf) throws IOException {
		FileSystem fs = null;
		Path tmpOutPath = null;
		try {
			Path out = new Path(outPath);
			fs = out.getFileSystem(conf);
			String tmpOutDir = outPath + "/tmp/";
			tmpOutPath = new Path(tmpOutDir);
			if (fs.exists(tmpOutPath)) {
				fs.delete(tmpOutPath, true);// true的意思是，就算output有东西，也一带删
			} else if (!fs.exists(tmpOutPath)) {
				HdfsUitls.createDir(fs, tmpOutPath);
				// fs.delete(tmpOutPath, true);// true的意思是，就算output有东西，也一带删
			}
			fs.deleteOnExit(tmpOutPath);

			// HdfsUitls.createDir(fs,tmpOutPath);
		} catch (Exception e) {
			tmpOutPath = null;
			e.printStackTrace();
		} finally {
			if (fs != null)
				fs.close();
		}
		return tmpOutPath;
	}

	/**
	 * 
	 * @param tmpOutPath
	 * @param conf
	 * @throws IOException
	 */
	@SuppressWarnings("static-access")
	public void moveDataFile(Path tmpOutPath, Configuration conf) {
		FileSystem fs = null;
		try {
			fs = tmpOutPath.getFileSystem(conf);
			FileStatus[] files = fs.listStatus(tmpOutPath);
			Path out = new Path(outPath);
			for (FileStatus file : files) {
				if (file.isFile()) {
					String fileName = file.getPath().getName();
					if (fileName.startsWith(filestartwith)) {
						Path dst = new Path(out + "/" + fileName);
						fs.rename(file.getPath(), dst);
					}
				}
			}
			// 删除临时目录
			if (fs.exists(tmpOutPath)) {
				fs.delete(tmpOutPath, true);// true的意思是，就算output有东西，也一带删
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fs != null) {
				try {
					fs.closeAll();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	@SuppressWarnings("static-access")
	private boolean checkInFiles(String inFiles,Configuration conf){
		boolean flag=true;
		FileSystem fs = null;
		try {
			String fileList[]=inFiles.split(",");
			Path in=new Path(inPath);
			fs=in.getFileSystem(conf);
			for(String f : fileList){
				Path fp = new Path(f);
				if(!fs.exists(fp)){
					flag=false;
					log.error(f +" 不存在，请检查");
					break;
				}
			}
		} catch (IOException e) {
			flag=false;
			e.printStackTrace();
		} finally {
			if (fs != null) {
				try {
					fs.closeAll();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}			
		return flag;
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		filestartwith = startStr.replace("_", "").replace("-", "");
		conf.set("filestartwith", filestartwith);
		conf.set("dilimer", dilimer);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, startStr);
		job.setJarByClass(EtlJob.class);
		job.setJobName("etl4lzofile"); // 设置一个用户定义的job名称
		log.warn("filestartwith:" + filestartwith);
		MultipleOutputs.addNamedOutput(job, filestartwith, TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.setCountersEnabled(job, true);

		job.setInputFormatClass(LzoTextInputFormat.class);
		job.setMapperClass(MapClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		// job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(0);
		List<CheckFileBean> lcheckFile = getVerifyFile(conf);
		if (lcheckFile.size() == 0) {
			log.error(String.format("在{}目录没有找到VERIFY_或VERIFY +{}的lzo校验文件", inPath, startStr));
			return -1;
		}
		String inFiles = getInputMapFileInfo(lcheckFile);
		if (inFiles.trim().length() == 0) {
			log.error("需要进行通过Map清洗的文件列表为空，请检查输入参数及输入路径");
			return -1;
		}
		if(!checkInFiles(inFiles,conf)){
			log.error("需要进行Map清洗的文件列表部分文件不存在,退出，请检查！");
			return -1;
		}
		log.info("需要进行通过Map清洗的文件列表:\n " + inFiles);

		Path tmpOutPath = createTmpOutDir(conf);
		FileInputFormat.setInputPaths(job, inFiles);
		FileOutputFormat.setOutputPath(job, tmpOutPath);
		int res = job.waitForCompletion(true) ? 0 : 1;
		if (res == 0) {
			moveDataFile(tmpOutPath, conf);

			// 针对Counter结果的显示
			Counters counters = job.getCounters();
			Counter rowNum = counters.findCounter(RowNumEnum.ROWCOUNTER);
			log.info("mapreduce clean reporter [MAPNUM:" + rowNum.getValue() + ",VERIFYSUMNUM:" + allRecordNum + "]");
			if (rowNum.getValue() != allRecordNum) {
				log.error("mapreduce clean reporter MAPNUM:" + rowNum.getValue() + "not equals VERIFYSUMNUM:"
						+ allRecordNum);
			}
//			Map<String,String> map=new HashMap<String,String>();
//			map.put("datadir",outPath);
//			map.put("verifynum", allRecordNum.toString());
//			map.put("mapnum", Long.toString(rowNum.getValue()));
//			System.setProperty("lzoinfo", map.toString());
//			System.getenv().put("lzoinfo", map.toString());
		}
		return 0;
	}
}
