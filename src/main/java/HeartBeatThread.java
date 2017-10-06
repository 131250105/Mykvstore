import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.rpc.RpcClientFactory;
import cn.helium.kvstore.rpc.RpcServer;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zy812818
 * Created @ 2017/10/6.
 **/
public class HeartBeatThread implements Runnable {

    private int obj = (RpcServer.getRpcServerId() + 2) % 3;

    private byte[] beat = new byte[]{2};

    private boolean coditioan = true;

    //落盘时用
    private MemTable perTable;

    private ReplicaTable replica;
    //以防主服务器在落盘时挂掉
    private ReplicaTable perReplica;

    private Gson gson = new Gson();

    @Override
    public void run() {

        while (coditioan) {
            try {
                RpcClientFactory.inform(obj, beat);
            } catch (IOException E) {
                //只是网路通信断了，主服务器没挂，备份的不落盘(仅在测试用例条件下，分区了就没有服务器挂掉，否则这里也要落盘)
                if (KvStoreConfig.getServersNum() == Config.INIT_SERVER_NUM)
                    continue;
                    //主服务器挂了,要落盘，并且承担主服务器的备份任务
                else if (KvStoreConfig.getServersNum() < Config.INIT_SERVER_NUM) {
                    //先转换格式
                    perTable = new MemTable();
                    addToPerMemTable(replica.getTable());
                    if (perReplica != null)
                        addToPerMemTable(perReplica.getTable());
                    //此处将memTable落盘

                    //更换心跳对象
                    obj = (RpcServer.getRpcServerId() + 1) % 3;
                }
            }
        }
    }

    public void setReplica(ReplicaTable replica) {
        this.replica = replica;
    }

    public void setPerReplica(ReplicaTable perReplica) {
        this.perReplica = perReplica;
    }


    public void addToPerMemTable(List<byte[]> data) {
        for (byte[] b : data) {

            Map<String, Map<String, String>> map = gson.fromJson(new String(b), new TypeToken<Map<String, Map<String, String>>>() {
            }.getType());//这是的new String是个问题，可以直接排序

            for(String s:map.keySet()){
                perTable.put(s,b);
            }
        }
    }
}
