import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.rpc.RpcServer;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by zy812818 
 * Created @ 2017/10/3.
 **/
public class Config {

    public static int MAX_NUM = 1000;

//    public static final int MAX_BLOCK_NUM =10;

    public static final int MAX_DATA_BLOCK_SIZE = 15*1024;

    public static final int INIT_SERVER_NUM = KvStoreConfig.getServersNum();

//    public static final String HDFS_URL = "hdfs://127.0.0.1:9";

    public static final String HDFS_URL = KvStoreConfig.getHdfsUrl();

    public static final String DIR ="/myKVStore";

//    public static final String LOG = "/opt/localdisk/Logs";

    public static final String LOG = "Logs";

    public static final String SPLIT = "_";

    public static final double loadFactor = 0.7;

//    public static final byte KV_NUM = Util.intToByteArray(RpcServer.getRpcServerId())[0];

//    public static final int KV_NUM = RpcServer.getRpcServerId();

//    public static final byte KV_NUM = 1;

    public static final int SERVER_NUM = 3;
//    public static final String hdfsUrl = KvStoreConfig.getHdfsUrl();

    public static final  int INDEX_SIZE = 1000000;

    public static final int SLOT_SIZE = 4;

    public static void expasionMaxNum(){
        MAX_NUM*=2;
    }

}
