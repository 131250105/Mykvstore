import cn.helium.kvstore.common.KvStoreConfig;

/**
 * Created by zy812818
 * Created @ 2017/10/3.
 **/
public class Config {

    public static int MAX_NUM = 1000;

    public static final int MAX_BLOCK_NUM =10;

    public static final int INIT_SERVER_NUM = KvStoreConfig.getServersNum();

    public static void expasionMaxNum(){
        MAX_NUM*=2;
    }
}
