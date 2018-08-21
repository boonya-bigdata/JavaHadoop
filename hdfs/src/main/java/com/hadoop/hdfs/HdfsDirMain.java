package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import java.io.IOException;
import java.util.Scanner;

/**
 * @ClassName: HdfsDirMain
 * @Description: TODO(功能描述:HDFS文件目录操作测试)
 * @author: pengjunlin
 * @company: 上海势航网络科技有限公司
 * @date 2018-06-22
 */
public class HdfsDirMain {
    /**
     * 创建文件
     * @throws Exception
     */
    private static void mkdir() throws Exception{
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path("hadoop_test");
        fileSystem.mkdirs(path);
    }

    /**
     * 删除文件
     * @throws IOException
     */
    private static void deletedir() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fileSystem =FileSystem.get(conf);
        Path path = new Path("hadoop_test");
        fileSystem.delete(path,true);
    }

    /**
     * 写文件
     * @throws IOException
     */
    private static void writerFile() throws IOException{
        Configuration conf = new Configuration();
        FileSystem fileSystem =FileSystem.get(conf);
        Path path = new Path("hadoop_test.txt");
        FSDataOutputStream dataOutputStream = fileSystem.create(path);
        dataOutputStream.writeUTF(" hello hadoop test");
    }

    /**
     * 读文件
     * @throws IOException
     */
    private static void readFile() throws IOException{
        Configuration conf = new Configuration();
        FileSystem fileSystem =FileSystem.get(conf);
        Path path = new Path("hadoop_test.txt");
        if(fileSystem.exists(path)){
            FSDataInputStream dataInputStream = fileSystem.open(path);
            FileStatus fileStatus = fileSystem.getFileStatus(path);
            byte b[] = new byte[Integer.parseInt(String.valueOf(fileStatus.getLen()))];
            dataInputStream.readFully(b);
        }
    }

    /**
     * 遍历目录文件
     * @throws IOException
     */
    private static void listDir() throws IOException{
        Configuration conf = new Configuration();
        FileSystem fileSystem =FileSystem.get(conf);
        Path path = new Path("/");

        FileStatus []fileStatus =  fileSystem.listStatus(path);
        print(fileStatus);
    }

    private static void print(FileStatus []fileStatus){
        for (int i = 0; i < fileStatus.length; i++) {
            FileStatus status = fileStatus[i];
            if(status.isDir()){

            }else{
                System.out.println(status.getPath().getName());
            }
        }
    }

    /**
     * 文件位置
     * @throws Exception
     */
    private static void fileLocal() throws Exception{
        Configuration conf = new Configuration();
        FileSystem fileSystem =FileSystem.get(conf);
        Path path = new Path("/user/centos");
        FileStatus fileStatus = fileSystem.getFileStatus(path);

        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0,fileStatus.getLen());

        for (int i = 0; i < blockLocations.length; i++) {
            String hosts[] = blockLocations[i].getHosts();
            for (int j = 0; j < hosts.length; j++) {
                System.out.println(hosts[j]+""+blockLocations[i].getLength());
            }
        }
    }

    /**
     * 节点信息
     * @throws IOException
     */
    private static void nodeDesc() throws IOException{
        Configuration conf = new Configuration();
        FileSystem fileSystem =FileSystem.get(conf);

        DistributedFileSystem distributedFileSystem =(DistributedFileSystem)fileSystem;
        DatanodeInfo datanodeInfo [] = distributedFileSystem.getDataNodeStats();
        for (int i = 0; i < datanodeInfo.length; i++) {
            System.out.println(datanodeInfo[i].getHostName() +"  "+datanodeInfo[i].getNonDfsUsed());
        }
    }

    /**
     * 注意，不能这么运行
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String arg ="";
        Scanner scanner = new Scanner(System.in);

        System.out.println("1、创建文件");
        System.out.println("2、删除文件");
        System.out.println("3、文件位置");
        System.out.println("4、遍历目录文件");
        System.out.println("5、节点信息");
        System.out.println("6、读文件");
        System.out.println("7、写文件");

        boolean isconture = true;
        while(isconture){
            arg = scanner.next();
            if("1".equals(args)){
                mkdir();
            }else if("2".equals(args)){
                deletedir();
            }else if("3".equals(args)){
                fileLocal();
            }else if("4".equals(args)){
                listDir();
            }else if("5".equals(args)){
                nodeDesc();
            }else if("6".equals(args)){
                readFile();
            }else if("7".equals(args)){
                writerFile();
            }

            System.out.println("是否继续 y/n");
            if(scanner.next().equals("n")){
                isconture = false;
            }
        }
        scanner.close();
    }
}
