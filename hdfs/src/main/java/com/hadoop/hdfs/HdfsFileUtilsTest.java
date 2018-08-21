package com.hadoop.hdfs;

import com.hadoop.hdfs.utils.HdfsFileUtils;

/**
 * @ClassName: HdfsFileUtilsTest
 * @Description: TODO(功能描述:HDFS文件操作测试)
 * @author: pengjunlin
 * @company: 上海势航网络科技有限公司
 * @date 2018-06-20
 */
public class HdfsFileUtilsTest {

    /**
     * 函数入口
     * @param args
     */
    public static void main(String[] args) throws Exception {
        //上传文件
        HdfsFileUtils.uploadFile("D:/input.txt","/input.txt");
        //下载文
        HdfsFileUtils.dowloadFile("/input.txt","D:/input_download.txt");
    }
}
