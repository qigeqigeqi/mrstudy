package com.atguigu.mr.compress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class TestCompress {

	public static void main(String[] args) throws Exception {
		
		// 压缩
//		compress("e:/hello.txt","org.apache.hadoop.io.compress.BZip2Codec");
//		compress("e:/hello.txt","org.apache.hadoop.io.compress.GzipCodec");
//		compress("e:/hello.txt","org.apache.hadoop.io.compress.DefaultCodec");
		
		decompress("e:/hello.txt.deflate");
		
	}

	@SuppressWarnings("resource")
	private static void decompress(String fileName) throws IOException {
		
		// 1 压缩方式检查
		CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
		CompressionCodec codec = factory.getCodec(new Path(fileName));//检验被解压对象是否能解压
		
		if (codec == null) {
			System.out.println("can not process");
			return;
		}
		
		// 2 获取输入流
		FileInputStream fis = new FileInputStream(new File(fileName));
		CompressionInputStream cis = codec.createInputStream(fis);//创建可解压的输入流
		
		// 3 获取输出流
		FileOutputStream fos = new FileOutputStream(new File(fileName+".decode"));
		
		// 4 流的对拷
		IOUtils.copyBytes(cis, fos, 1024*1024, false);
		
		// 5 关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(cis);
		IOUtils.closeStream(fis);
	}

	@SuppressWarnings({ "resource", "unchecked" })
	private static void compress(String fileName, String method) throws ClassNotFoundException, IOException {
		
		// 1 获取输入流
		FileInputStream fis = new FileInputStream(new File(fileName));

		//以下操作是获取压缩
		Class classCodec = Class.forName(method);//通过反射获取压缩类
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(classCodec, new Configuration());
		
		// 2 获取输出流
		FileOutputStream fos = new FileOutputStream(new File(fileName+codec.getDefaultExtension()));//加文件后缀
		CompressionOutputStream cos = codec.createOutputStream(fos);//通过压缩方式创建一个能压缩的输出流
		
		// 3 流的对拷
		IOUtils.copyBytes(fis, cos, 1024*1024, false);
		
		// 4 关闭资源
		IOUtils.closeStream(cos);
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
	}
}
