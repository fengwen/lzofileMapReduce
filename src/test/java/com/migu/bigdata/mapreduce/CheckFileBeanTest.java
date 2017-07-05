package com.migu.bigdata.mapreduce;

import com.migu.bigdata.bean.CheckFileBean;

public class CheckFileBeanTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String l="APPEAR_SER_DAY_ADD_2017070100.0000.dat€30001000€500000€2017070100€20170701020132";
		CheckFileBean cf=new CheckFileBean(l);
		System.out.print(cf.printRows());
	}

}
