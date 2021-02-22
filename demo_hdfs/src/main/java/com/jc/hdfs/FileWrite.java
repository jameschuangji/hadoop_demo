package com.jc.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class FileWrite {

    public static void main(String[] args) {
        if(args == null || args.length!=2){
            System.out.println("错误的使用方式：hdfsfilewrite [本地文件地址] [hdfs目的文件地址]");
            return;
        }

        Configuration conf = new Configuration();
        try{
            //指定hdfs入口地址
            conf.set("fs.defaultFS","hdfs://xxx.xxx.xxx.xxx.9820");
            FileSystem fs = FileSystem.get(conf);

            //构造输入流
            String inFilePath = args[0];
            File inFile = new File(inFilePath);
            if (!inFile.exists()){
                System.out.println("Output file already exists" + inFilePath);
                throw new IOException("Output file already exists");
            }
            InputStream in = new BufferedInputStream(new FileInputStream(inFile));

            //构造输出流
            Path outFile = new Path(args[1]);
            FSDataOutputStream out = fs.create(outFile);

            //开始写入
            byte buffer[] = new byte[256];
            try {
                int bytesRead = 0;
                while ((bytesRead =in.read(buffer)) >0){
                    out.write(buffer,0,bytesRead);
                }
            }catch (IOException e){
                System.out.println("Error whil copying file");
            }finally {
                in.close();
                out.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
