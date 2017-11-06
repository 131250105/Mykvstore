import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zy812818
 * Created @ 2017/10/14.
 **/
public class FileMap {


    //日志所在路径
    public static HashMap<String,KLog> KLogMap = new HashMap<>();
    //索引文件
    public static HashMap<Integer,String> indexFiles = new HashMap<>();
    //data所在文件
    public static HashMap<Integer,String> dataFiles = new HashMap<>();
    //每个dataFile中写到的偏移量
    public static HashMap<Integer,Long> offsets = new HashMap<>();
    //len对应IndexhashTable
    public static HashMap<Integer,IndexHashTable> indexTables = new HashMap<>();

    public static HashMap<Integer,HDFSWriteThread> threads = new HashMap<>();

    public static KLog getKLog(String logId){
        if(KLogMap.get(logId) != null){
            return KLogMap.get(logId);
        }
        else {
            KLog log = new KLog(logId);
            synchronized (KLogMap){
                if(KLogMap.get(logId) == null){
                    KLogMap.put(logId,log);
                    return log;
                }else{
                    return KLogMap.get(logId);
                }
            }
        }
    }
}
