package com.hadoop.hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import java.util.Iterator;
import java.util.Map;

/**
 * @ClassName: HdfsSafeUtils
 * @Description: TODO(功能描述:HDFS同步安全操作工具类)
 * @author: pengjunlin
 * @company: 上海势航网络科技有限公司
 * @date 2018-06-22
 */
public class HdfsSafeUtils {

    /**
     * 获取文件系统
     * @param ip
     * @param port
     * @return
     */
    public synchronized static FileSystem getFileSystem(String ip, int port) {
        FileSystem fs = null;
        String url = "hdfs://" + ip + ":" + String.valueOf(port);
        Configuration config = new Configuration();
        config.set("fs.defaultFS", url);
        try {
            fs = FileSystem.get(config);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fs;
    }

    /**
     * 遍历HDFS文件目录
     * @param fs
     */
    public synchronized static void listNode(FileSystem fs) {
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        try {
            DatanodeInfo[] infos = dfs.getDataNodeStats();
            for (DatanodeInfo node : infos) {
                System.out.println("HostName: " + node.getHostName() + "/n"
                        + node.getDatanodeReport());
                System.out.println("--------------------------------");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 遍历HDFS文件目录状态
     * @param fs
     */
    public synchronized static void listStatus(FileSystem fs,String path) {
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        try {
            FileStatus[] statuses = dfs.listStatus(new Path(path));
            System.out.println("path status--------------------------------start!");
            for (FileStatus status : statuses) {
                System.out.println(status);
            }
            System.out.println("path status--------------------------------end!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 打印系统配置
     * @param fs
     */
    public synchronized static void listConfig(FileSystem fs) {
        Iterator<Map.Entry<String, String>> entrys = fs.getConf().iterator();
        while (entrys.hasNext()) {
            Map.Entry<String, String> item = entrys.next();
            System.out.println(item.getKey() + ": " + item.getValue());
        }
    }

    /**
     * 创建目录和父目录
     * @param fs
     * @param dirName
     */
    public synchronized static void mkdirs(FileSystem fs, String dirName) {
        // Path home = fs.getHomeDirectory();
        Path workDir = fs.getWorkingDirectory();
        String dir = workDir + "/" + dirName;
        Path src = new Path(dir);
        // FsPermission p = FsPermission.getDefault();
        boolean succ;
        try {
            succ = fs.mkdirs(src);
            if (succ) {
                System.out.println("create directory " + dir + " successed. ");
            } else {
                System.out.println("create directory " + dir + " failed. ");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除目录和子目录
     * @param fs
     * @param dirName
     */
    public synchronized static void rmdirs(FileSystem fs, String dirName) {
        // Path home = fs.getHomeDirectory();
        Path workDir = fs.getWorkingDirectory();
        String dir = workDir + "/" + dirName;
        Path src = new Path(dir);
        boolean succ;
        try {
            succ = fs.delete(src, true);
            if (succ) {
                System.out.println("remove directory " + dir + " successed. ");
            } else {
                System.out.println("remove directory " + dir + " failed. ");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 上传目录或文件
     * @param fs
     * @param local
     * @param remote
     */
    public synchronized static void upload(FileSystem fs, String local,
                                           String remote) {
        // Path home = fs.getHomeDirectory();
        Path workDir = fs.getWorkingDirectory();
        Path dst = new Path(workDir + "/" + remote);
        Path src = new Path(local);
        try {
            fs.copyFromLocalFile(false, true, src, dst);
            System.out.println("upload " + local + " to  " + remote + " successed. ");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 下载目录或文件
     * @param fs
     * @param local
     * @param remote
     */
    public synchronized static void download(FileSystem fs, String local,String remote) {
        // Path home = fs.getHomeDirectory();
        Path workDir = fs.getWorkingDirectory();
        Path dst = new Path(workDir + "/" + remote);
        Path src = new Path(local);
        try {
            fs.copyToLocalFile(false, dst, src);
            System.out.println("download from " + remote + " to  " + local
                    + " successed. ");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 字节数转换
     * @param size
     * @return
     */
    public synchronized static String convertSize(long size) {
        String result = String.valueOf(size);
        if (size < 1024 * 1024) {
            result = String.valueOf(size / 1024) + " KB";
        } else if (size >= 1024 * 1024 && size < 1024 * 1024 * 1024) {
            result = String.valueOf(size / 1024 / 1024) + " MB";
        } else if (size >= 1024 * 1024 * 1024) {
            result = String.valueOf(size / 1024 / 1024 / 1024) + " GB";
        } else {
            result = result + " B";
        }
        return result;
    }

    /**
     * 遍历HDFS上的文件和目录
     * @param fs
     * @param path
     */
    public synchronized static void listFile(FileSystem fs, String path) {
        Path workDir = fs.getWorkingDirectory();
        Path dst;
        if (null == path || "".equals(path)) {
            dst = new Path(workDir + "/" + path);
        } else {
            dst = new Path(path);
        }
        try {
            String relativePath = "";
            FileStatus[] fList = fs.listStatus(dst);
            for (FileStatus f : fList) {
                if (null != f) {
                    relativePath = new StringBuffer()
                            .append(f.getPath().getParent()).append("/")
                            .append(f.getPath().getName()).toString();
                    if (f.isDir()) {
                        listFile(fs, relativePath);
                    } else {
                        System.out.println(convertSize(f.getLen()) + "/t/t"
                                + relativePath);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
    }

    /**
     * 路径写入数据
     * @param fs
     * @param path
     * @param data
     */
    public synchronized static void write(FileSystem fs, String path,String data) {
        // Path home = fs.getHomeDirectory();
        Path workDir = fs.getWorkingDirectory();
        Path dst = new Path(workDir + "/" + path);
        try {
            FSDataOutputStream dos = fs.create(dst);
            dos.writeUTF(data);
            dos.close();
            System.out.println("write content to " + path + " successed. ");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 路径追加数据
     * @param fs
     * @param path
     * @param data
     */
    public synchronized static void append(FileSystem fs, String path,String data) {
        // Path home = fs.getHomeDirectory();
        Path workDir = fs.getWorkingDirectory();
        Path dst = new Path(workDir + "/" + path);
        try {
            FSDataOutputStream dos = fs.append(dst);
            dos.writeUTF(data);
            dos.close();
            System.out.println("append content to " + path + " successed. ");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 读取路径数据
     * @param fs
     * @param path
     * @return
     */
    public synchronized static String read(FileSystem fs, String path) {
        String content = null;
        // Path home = fs.getHomeDirectory();
        Path workDir = fs.getWorkingDirectory();
        Path dst = new Path(workDir + "/" + path);
        try {
            // reading
            FSDataInputStream dis = fs.open(dst);
            content = dis.readUTF();
            dis.close();
            System.out.println("read content from " + path + " successed. ");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return content;
    }
}
