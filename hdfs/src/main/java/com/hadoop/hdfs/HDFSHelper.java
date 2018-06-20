package com.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.URI;

/**
 * @ClassName: HDFSHelper
 * @Description: TODO(功能描述：HDFS文件助手(上传|下载|删除))
 * @author: pengjunlin
 * @company: 上海势航网络科技有限公司
 * @date 2018-06-20
 * @References https://blog.csdn.net/admin1973/article/details/60876255
 */
public class HDFSHelper {

    public static final String HDFS_BASE_PATH="hdfs://172.16.20.11:9000";

    public static final String HDFS_ROOT_USER="root";

    public static final Configuration CONFIGURATION=new Configuration();

    static {
        CONFIGURATION.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        CONFIGURATION.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }


    /**
     * 下载HDFS文件
     * @param inputPath
     * @param outputPath
     * @throws Exception
     */
    public static void dowloadFile(String inputPath,String outputPath)throws Exception{
        //FileSystem是一个抽象类,因此我们再使用它的时候要先创建FileSystem的实现类(工具类)
        FileSystem fs = FileSystem.get(new URI(HDFS_BASE_PATH),CONFIGURATION);
        InputStream is = fs.open(new Path(inputPath));
        OutputStream out = new FileOutputStream(outputPath);
        IOUtils.copyBytes(is,out,4096,true);
        System.out.println("下载完成");
    }

    /**
     * 上传文件至HDFS
     * @param inputPath
     * @param outputPath
     * @throws Exception
     */
    public static void uploadFile(String inputPath,String outputPath)throws Exception{
        FileSystem fs = FileSystem.get(new URI(HDFS_BASE_PATH),CONFIGURATION,HDFS_ROOT_USER);
        //读取本地文件系统,并创建输入流
        InputStream in = new FileInputStream(inputPath);
        //在HDFS上创建一个文件返回输出流
        OutputStream out = fs.create(new Path(outputPath));
        //将输入流写到输出流,buffersize是4k,即每读4k数据返回一次,写完返回true
        IOUtils.copyBytes(in,out,4096,true);
        System.out.println("上传Hadoop文件成功!");
    }

    /**
     * 删除HDFS文件
     * @param filePath
     * @throws Exception
     */
    public static void deleteFile(String filePath)throws Exception{
        FileSystem fs = FileSystem.get(new URI(HDFS_BASE_PATH),CONFIGURATION,HDFS_ROOT_USER);
        //测试删除文件,我们尝试删除HDFS下的sogou_pinyin_80k.exe,fs.delete()第二个参数是告诉方法是否
        //递归删除,如果是文件夹,并且文件夹中有文件的话就填写true,否则填false
        boolean flag =fs.delete(new Path(filePath),false);
        System.out.println("删除文件状态:"+flag);
    }
}
