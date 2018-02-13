package com.mljr.hdfs;
import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HdfsStatus {
    public static void main(String[] args) {
        FileSystem hdfs = null;
        try{
            Configuration config = new Configuration();

            hdfs = FileSystem.get(new URI("***"),//主节点ip或者hosts
                    config, "hdfs");
            Path path = new Path("/");
            String content_csv = "/home/dmp/jiansong.peng/content.csv";
            long startTime=System.currentTimeMillis();   //获取开始时间
            BufferedOutputStream out =new BufferedOutputStream(new FileOutputStream(new File(content_csv)));
            iteratorShowFiles(hdfs, path,out);
            out.close();
            long endTime=System.currentTimeMillis(); //获取结束时间
            long runTime = (endTime-startTime)/1000/60;
            System.out.println("程序运行时间： "+runTime+"min");

        }catch(Exception e){
            e.printStackTrace();
        }finally{
            if(hdfs != null){
                try {
                    hdfs.closeAll();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     *
     * @param hdfs FileSystem 对象
     * @param path 文件路径
     */
    public static void iteratorShowFiles(FileSystem hdfs, Path path,BufferedOutputStream out){


        String line = System.getProperty("line.separator");
        try{
            if(hdfs == null || path == null){
                return;
            }
            //获取文件列表
            FileStatus[] files = hdfs.listStatus(path);
            //创建输出文件

            //展示文件信息
            for (int i = 0; i < files.length; i++) {
                try{
                    if(files[i].isDirectory()){
                        String text = (files[i].getPath().toString().replace("hdfs://nameservice1","")
                                + "," + files[i].getOwner()
                                + "," + "0"
                                + "," + "0"
                                + "," + files[i].getBlockSize()
                                + "," + files[i].getPermission()
                                + "," + files[i].getAccessTime()
                                + "," + files[i].getModificationTime()
                                + "," + files[i].getReplication()+line);
                        out.write(text.getBytes());
                        //递归调用
                        iteratorShowFiles(hdfs, files[i].getPath(),out);
                    }else if(files[i].isFile()){
                        String text=files[i].getPath().toString().replace("hdfs://nameservice1","")
                                + "," + files[i].getOwner()
                                + "," + "1"
                                + "," + files[i].getLen()
                                + "," + files[i].getBlockSize()
                                + "," + files[i].getPermission()
                                + "," + files[i].getAccessTime()
                                + "," + files[i].getModificationTime()
                                + "," + files[i].getReplication()+line;
                        out.write(text.getBytes());
                    }
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
