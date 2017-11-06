import java.io.*;

/**
 * Created by H77 on 2017/10/21.
 */
//对每一条记录都会先记录Log
public class KLog {
    public String addr;
    public String logId;
    public int num;
    public String preAddr;
    public RandomAccessFile  randomFile = null;
    public KLog(String logId)  {
        num = 0;
        this.logId = logId;
        this.addr =Config.LOG+"/"+logId+"_"+num;
        try {
            randomFile = new RandomAccessFile(this.addr,"rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }
    public void append(byte[] log){
        try {
            randomFile.write(log);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
//    public void deleteLog(){
//        LocalFSConn.deleteFile(this.preAddr);
//    }

    public void switchLog(HDFSWriteObject object)  {

        object.setPath(this.addr);
//        this.preAddr = this.addr;
        this.addr = Config.LOG+"/"+logId+"_"+num++;
        try {
            randomFile.close();
            randomFile = new RandomAccessFile(this.addr,"rw");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
