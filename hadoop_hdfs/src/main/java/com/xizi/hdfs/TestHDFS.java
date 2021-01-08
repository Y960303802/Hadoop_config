package com.xizi.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestHDFS {

    private FileSystem fileSystem;

    @Before
    public void before() throws IOException {
        //将window 用户名在运行时修改为root用户
//        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hadoop1:9000");
        this.fileSystem = FileSystem.get(configuration);
    }

    @After
    public void after() throws IOException {
        fileSystem.close();
    }


    //上传文件
    @Test
    public void testUpload() throws IOException {
        //上传到hdfs目录
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/aaa222.log"));
        //读取本地文件
        FileInputStream fileInputStream = new FileInputStream("D:\\Documents\\Desktop\\aaa.txt");
        //复制流的操作
        IOUtils.copyBytes(fileInputStream, fsDataOutputStream,1024,true );
    }

    //下载文件
    // 1.第一种方式
    @Test
    public void testDownload() throws IOException {
        Path source = new Path("/xizi.xml");
        Path des = new Path("D:\\Documents\\Desktop\\");
        fileSystem.copyToLocalFile(false,source,des,true);
    }
    // 2.第二种方式
    @Test
    public void testDownload1() throws IOException {
        Path path = new Path("/xizi.xml");
        FSDataInputStream in = fileSystem.open(path);
        FileOutputStream os = new FileOutputStream("D:\\Documents\\Desktop\\");
        IOUtils.copyBytes(in,os,1024,true);
    }
    // 展示hdfs目录和文件
    @Test
    public void testListDirs() throws IOException {
        Path path = new Path("/");
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.isDirectory()+" "+fileStatus.getPath());
        }
    }
    //创建文件目录
    @Test
    public void testMkdirs() throws IOException {
        boolean mkdirs = fileSystem.mkdirs(new Path("/aa/cc/cc"));
        System.out.println("mkdirs = " + mkdirs);
    }

    //删除文件
    @Test
    public void testDelete() throws IOException {
        Path path= new Path("/aa");
        //参数1:目录路径  参数2:是否递归删除
        fileSystem.delete(path,true);
    }

    //展示hdfs文件列表
    @Test
    public void testListFiles() throws IOException {
        Path path = new Path("/");
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(path, true);
        while (listFiles.hasNext()){
            LocatedFileStatus next = listFiles.next();
            System.out.println("next = " + next);
        }
    }

}
