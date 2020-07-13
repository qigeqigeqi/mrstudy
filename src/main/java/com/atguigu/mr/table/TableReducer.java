package com.atguigu.mr.table;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<TableBean> values,
			Context context) throws IOException, InterruptedException {
		
		// 存储所有订单集合
		ArrayList<TableBean> orderBeans = new ArrayList<>();
		// 存储产品信息
		TableBean pdBean = new TableBean();
		
		for (TableBean tableBean : values) {
			
			if ("order".equals(tableBean.getFlag())) {// 订单表
				
				TableBean tmpBean = new TableBean();
				
				try {
					BeanUtils.copyProperties(tmpBean, tableBean);//tableBean只是一个引用，需要将其放到一一个临时对象中
					
					orderBeans.add(tmpBean);//如果不添加上述操作，最终只会添加最后一个bead对象，因为上述操作的tableBean只是一个引用
					
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
			}else {
				try {
					BeanUtils.copyProperties(pdBean, tableBean);//这里不需要临时对象，因为产品表就一个对象
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
			}
		}
		
		
		for (TableBean tableBean : orderBeans) {
			tableBean.setPname(pdBean.getPname());
			
			context.write(tableBean, NullWritable.get());//tableBean重写了toString方法，只会打印需求的属性
		}
	}
}
