/**
 * Created by zy812818
 * Created @ 2017/9/30.
 **/

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.zookeeper.common.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;


public class MyTestHDFSConn {


    static {
        try {
            mkdir(Config.DIR);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     新建文件夹
     */
    public static boolean mkdir(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            return false;
        }
        dir = Config.HDFS_URL + dir;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dir), conf);
        if (!fs.exists(new Path(dir))) {
            fs.mkdirs(new Path(dir));
            fs.close();
            return true;
        }else{
            fs.close();
            return false;
        }
    }

    /**
     删除文件夹
     *
     */
    public static boolean deleteDir(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            return false;
        }
        dir = Config.HDFS_URL + dir;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dir), conf);
        fs.delete(new Path(dir), true);
        fs.close();
        return true;
    }



    /**
     列出文件夹下所有文件
     */
    public static List<String> listAll(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            return new ArrayList<String>();
        }
        dir = Config.HDFS_URL + dir;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dir), conf);
        FileStatus[] stats = fs.listStatus(new Path(dir));
        List<String> names = new ArrayList<String>();
        for (int i = 0; i < stats.length; ++i) {
            if (stats[i].isFile()) {
                // regular file
                names.add(stats[i].getPath().toString());
            } else if (stats[i].isDirectory()) {
                // dir
                names.add(stats[i].getPath().toString());
            } else if (stats[i].isSymlink()) {
                // is s symlink in linux
                names.add(stats[i].getPath().toString());
            }
        }

        fs.close();
        return names;
    }


    /**
     列出文件夹下文件数量
     */
    public static int fileNum(String dir) throws IOException{
        if (StringUtils.isBlank(dir)) {
            return 0;
        }
        dir = Config.HDFS_URL + dir;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dir), conf);
        FileStatus[] stats = fs.listStatus(new Path(dir));
        return  stats.length;
    }
    /**
      创建文件
     */
    public static boolean createNewHDFSFile(String newFile, byte[] content) throws IOException {
        if (StringUtils.isBlank(newFile) || null == content) {
            return false;
        }
        newFile = Config.HDFS_URL + newFile;
        Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create(newFile), config);
        FSDataOutputStream os = hdfs.create(new Path(newFile));
        os.write(content);
        os.close();
        hdfs.close();
        return true;
    }

    /**
     删除文件
     */
    public static boolean deleteHDFSFile(String hdfsFile) throws IOException {
        if (StringUtils.isBlank(hdfsFile)) {
            return false;
        }
        hdfsFile = Config.HDFS_URL + hdfsFile;
        Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create(hdfsFile), config);
        Path path = new Path(hdfsFile);
        boolean isDeleted = hdfs.delete(path, true);
        hdfs.close();
        return isDeleted;
    }

    /**
     读文件
     */
    public static byte[] readHDFSFile(String hdfsFile) throws Exception {
        if (StringUtils.isBlank(hdfsFile)) {
            return null;
        }
        if(!hdfsFile.contains("hdfs")) {
            hdfsFile = Config.HDFS_URL + hdfsFile;
        }
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfsFile), conf);
        // check if the file exists
        Path path = new Path(hdfsFile);
        if (fs.exists(path)) {
            FSDataInputStream is = fs.open(path);
            // get the file info to create the buffer
            FileStatus stat = fs.getFileStatus(path);
            // create the buffer
            byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
            is.readFully(0, buffer);
            is.close();
            fs.close();
            return buffer;
        } else {
            throw new Exception("the file is not found .");
        }
    }

    public static byte[] readByOffset(String hdfsFile , long offset ,int len) throws Exception{
        if (StringUtils.isBlank(hdfsFile)) {
            return null;
        }
        if(!hdfsFile.contains("hdfs")) {
            hdfsFile = Config.HDFS_URL + hdfsFile;
        }
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfsFile), conf);
        // check if the file exists
        Path path = new Path(hdfsFile);
        byte[] contents = new byte[len];
        if (fs.exists(path)) {
            FSDataInputStream is = fs.open(path);
            is.skip(offset);
            is.read(contents);
        }
        return contents;
    }

    /**
     在文件末尾增加内容 用来写data文件
     */
    public static void append(String hdfsFile ,byte[] content,int contentLength) throws Exception {
        if (StringUtils.isBlank(hdfsFile)) {
            return ;
        }
        if(content == null || content.length==0){
            return ;
        }
        String _hdfsFile = Config.HDFS_URL + hdfsFile;
        Configuration conf = new Configuration();
        // solve the problem when appending at single datanode hadoop env
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        FileSystem fs = FileSystem.get(URI.create(_hdfsFile), conf);
        // check if the file exists
        Path path = new Path(_hdfsFile);
        if (fs.exists(path)) {
            try {
//                InputStream in = new ByteArrayInputStream(content);
                OutputStream out = fs.append(new Path(_hdfsFile));
//                IOUtils.copyBytes(in, out, 4096, true);
                out.write(content,0,contentLength);
//                out.write(content,offset,content.length);
                out.close();
                fs.close();
//                in.close();
//                fs.close();
            } catch (Exception ex) {
                fs.close();
                throw ex;
            }
        } else {
            createNewHDFSFile(hdfsFile, content);
        }
    }



    public static long getFileLength(String path) throws Exception{
        if (StringUtils.isBlank(path)) {
            return 0;
        }
        if(!path.contains("hdfs")) {
            path = Config.HDFS_URL + path;
        }
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(path), conf);
        Path hdfspath = new Path(path);
        FileStatus status = fs.getFileStatus(hdfspath);
        return status.getLen();
    }

    public static void main(String[] args) throws Exception{
//        mkdir(parentDir);
//        createNewHDFSFile(parentDir+"/test","hello,this is a test");

//        byte[] result = readHDFSFile(parentDir+"/test");
//        System.out.println(new String(result));
    }
}
