package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;

/**
 * @ClassName: HDFSMain
 * @Description: TODO(功能描述:HDFS文件操作测试)
 * @author: pengjunlin
 * @company: 上海势航网络科技有限公司
 * @date 2018-06-20
 */
public class HDFSMain {

    public static void main(String[] args) throws  Exception{

        //上传文件
        HDFSHelper.uploadFile("D:/input.txt","/input.txt");

        //下载文件
        HDFSHelper.dowloadFile("/input.txt","D:/input_download.txt");

    }
}
