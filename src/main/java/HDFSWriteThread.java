import cn.helium.kvstore.rpc.RpcServer;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by zy812818
 * Created @ 2017/11/5.
 **/
public class HDFSWriteThread implements Runnable{

    private LinkedBlockingQueue<HDFSWriteObject> queue = new LinkedBlockingQueue();

    private int len ;


    public HDFSWriteThread(int len) {
        this.len = len;
    }

    @Override
    public void run(){

        try {
            while (true) {
                HDFSWriteObject object = queue.take();
                this.putData(object.getValueContent(),object.getIndexContent());
                LocalFSConn.deleteFile(object.getPath());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private boolean putData(byte[] valContents ,byte[] indexContents) {

        String fileIndex = FileMap.indexFiles.getOrDefault(len, null);
        String fileData = null;
        if (null == fileIndex) {
            fileIndex = Config.DIR + "/" + "index" + Config.SPLIT + len +Config.SPLIT + RpcServer.getRpcServerId();
            fileData = Config.DIR + "/" + "data" + Config.SPLIT + len + Config.SPLIT + RpcServer.getRpcServerId();
            FileMap.indexFiles.put(len, fileIndex);
            FileMap.dataFiles.put(len, fileData);
        } else {
            fileData = FileMap.dataFiles.get(len);
        }
        try {
            //将文件和对应的索引文件落盘
            MyTestHDFSConn.append(fileData, valContents, valContents.length);
            MyTestHDFSConn.append(fileIndex, indexContents,indexContents.length);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }


    public void addObject(HDFSWriteObject object){
        try {
            queue.put(object);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
