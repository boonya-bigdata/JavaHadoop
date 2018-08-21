package com.hadoop.hdfs;

import com.hadoop.hdfs.utils.HdfsFileUtils;

/**
 * @ClassName: HdfsFileMain
 * @Description: TODO(功能描述:HDFS文件操作测试)
 * @author: pengjunlin
 * @company: 上海势航网络科技有限公司
 * @date 2018-06-21
 */
public class HdfsFileMain {

    /**
     * 函数入口
     * @param args
     */
    public static void main(String[] args) {
        // args[0] operate
        // args[1] input
        // args[2] output
        if(args.length!=3) {
            System.out.println("operate=args[0] int {1:'download',2:'upload'},input=args[1] string ,output=args[2] string ,args.length!=3");
            System.out.println("=============================================");
            return;
        }
        int operate;
        try{
            operate=Integer.parseInt(args[0]);
        }catch (Exception ex){
            System.out.println("args[0] must is int value");
            return ;
        }
        try{
            switch (operate){
                case 1:
                    HdfsFileUtils.dowloadFile(args[1] ,args[2] );
                    break;
                case 2:
                    HdfsFileUtils.uploadFile(args[1] ,args[2] );
                    break;
                default:
                    System.out.println("args[0] operate is not support");
                    break;
            }
        }catch (Exception ex){
            System.out.println("Operate exception:"+ex.getMessage());
        }


    }
}
