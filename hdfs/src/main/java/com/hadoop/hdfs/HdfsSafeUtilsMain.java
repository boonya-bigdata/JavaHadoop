package com.hadoop.hdfs;

import com.hadoop.hdfs.utils.HdfsSafeUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
/**
 * @ClassName: HdfsSafeUtilsmain
 * @Description: TODO(功能描述:HDFS同步安全操作工具类测试)
 * @author: pengjunlin
 * @company: 上海势航网络科技有限公司
 * @date 2018-06-22
 */
public class HdfsSafeUtilsMain {

    /**
     * 函数入口
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        FileSystem fs = HdfsSafeUtils.getFileSystem("172.16.20.11", 9000);
        HdfsSafeUtils.listNode(fs); //打印各个node信息
        String Dir = "input";
        String FileName = "hdfs-name.txt";

        try {
            if(!fs.exists(new Path("input")))
            {
                HdfsSafeUtils.mkdirs(fs, Dir);
                System.out.println("mkdir" + Dir);
            }
            else
            {
                System.out.println( Dir + " exists!");
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        HdfsSafeUtils.write(fs, Dir+"/"+FileName, "Dior Messi"); //会重写的
//      HDFSUtil.append(fs, Dir+"/"+FileName, "/ntest-测试2"); //最好不用对hdfs文件进行追加操作。支持 性不好

        System.out.println("write " + Dir+"/"+FileName);
        String sFileContend = HdfsSafeUtils.read(fs, Dir+"/"+FileName);
        System.out.println(sFileContend);
        System.out.println("read " + Dir+"/"+FileName);
    }
}
