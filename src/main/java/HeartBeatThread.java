import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.rpc.RpcClientFactory;
import cn.helium.kvstore.rpc.RpcServer;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Created by zy812818
 * Created @ 2017/10/6.
 **/
public class HeartBeatThread implements Runnable {

    private int obj = (RpcServer.getRpcServerId() + 2) % 3;

    private byte[] beat = new byte[]{2};

    private boolean coditioan = true;

    private Queue<ReplicaTable> replicas;

    private MyProcessor processor;

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
//                    this.save();
                    //更换心跳对象
                    obj = (RpcServer.getRpcServerId() + 1) % 3;
                }
            }
        }
    }

    public void setReplicas(Queue<ReplicaTable> replicas) {
        this.replicas = replicas;
    }

//    public void save() {
//        StringBuilder sb = new StringBuilder();
//
//        ReplicaTable replicaTable = null;
//        while ((replicaTable = replicas.poll()) != null) {
//            for (byte[] bytes : replicaTable.getTable()) {
//                int keyLength = 0;
//                for (int i = 0; i < bytes.length; i++) {
//                    if (bytes[i] != -1) {
//                        keyLength++;
//                        sb.append(bytes[i]);
//                    } else {
//                        break;
//                    }
//                }
//                byte[] value = new byte[bytes.length - keyLength - 1];
//                System.arraycopy(bytes, keyLength + 1, value, 0, value.length);
//                sb.setLength(0);
//                processor.dealSelf(sb.toString(), value);
//            }
//        }
//    }

    public void setProcessor(MyProcessor processor) {
        this.processor = processor;
    }
}
