import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Created by zy812818
 * Created @ 2017/11/5.
 **/
public class HDFSReadThread implements Runnable{


    private CountDownLatch countDownLatch;

    private String Id;

    private ConcurrentHashMap<String, HashMap<String, byte[]>> indexTables;

    private HashMap<String, String> globalIndexFiles ;

    public HDFSReadThread(CountDownLatch countDownLatch, ConcurrentHashMap<String, HashMap<String, byte[]>> indexTables, String Id, HashMap<String, String> globalIndexFiles){
        this.countDownLatch = countDownLatch;
        this.indexTables = indexTables;
        this.Id = Id;
        this.globalIndexFiles = globalIndexFiles;

    }

    @Override
    public void run()  {
        try {
            byte[] contents = MyTestHDFSConn.readHDFSFile(globalIndexFiles.get(Id));
            parseIndexFile(Id, contents);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            countDownLatch.countDown();
        }
    }


    private void parseIndexFile(String id, byte[] contents) {
        int size = contents.length;
        int offset = 0;
        int keyoffset = 0;
        //8 是offset+len的长度  loadFactor
//        int slotsize = len+8;
//        int tablesize = ((int)(contents.length / Config.loadFactor)/slotsize)*slotsize+slotsize;
//        byte[] table = new byte[tablesize];
//        IndexHashTable indexTable = new IndexHashTable(len,table ,slotsize);
//        indexTables.put(len,indexTable);
        HashMap<String, byte[]> indexTable = new HashMap<>();
        indexTables.put(id, indexTable);
        while (offset < size) {
            int length = contents[offset++];
            keyoffset = offset + length - 8-4;
            byte[] key_content = Arrays.copyOfRange(contents, offset, keyoffset);
            offset = offset + length;
            byte[] offset_len = Arrays.copyOfRange(contents, keyoffset, offset);
            indexTable.put(new String(key_content), offset_len);
        }
    }
}
