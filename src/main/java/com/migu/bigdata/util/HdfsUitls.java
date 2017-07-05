/**
 * 操作hdfs文件系统
 * @author FengL
 */
package com.migu.bigdata.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

public class HdfsUitls {
	/**
	 * 创建目录
	 * 
	 * @param fs
	 * @param path
	 * @throws IOException
	 */
	public static void createDir(FileSystem fs, Path path) throws IOException {
		fs.create(path);
	}

	public static void deleteDir(FileSystem fs, Path path) {

	}

	/**
	 * 将某个目录下文件迁移到另一个目录
	 * 
	 * @param fileNameStart
	 */
	public static void mvfile(String sourceDir, String targetDir, String fileNameStart, FileSystem fs) {

	}

	public static void mkdirs(FileSystem fs, Path path) throws IOException {
		FsPermission filePermission = null;
		filePermission = new FsPermission(FsAction.ALL, // user action
				FsAction.ALL, // group action
				FsAction.ALL);// other action
		boolean success = fs.mkdirs(path, filePermission);
	}
}
