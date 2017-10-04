/**
 * Created by zy812818
 * Created @ 2017/9/27.
 **/

import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.processor.Processor;
import cn.helium.kvstore.rpc.RpcClientFactory;
import cn.helium.kvstore.rpc.RpcServer;
import com.google.gson.Gson;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class MyProcessor implements Processor {

    //默认的无参构造函数应该也可以被反射调用
    //由于题目中给出的kvstoreserver好像shut down了之后不会再开启，所以第一版先不考虑op log
    private ExecutorService threadPool = Executors.newSingleThreadExecutor();
    private MyTestHDFSConn conn = new MyTestHDFSConn(KvStoreConfig.getHdfsUrl());
    private Gson gson = new Gson();
    private MemTable first = new MemTable();
    private MemTable second = new MemTable();
    private HashMap<String,Map<String, String>> tmpMap = new HashMap<>();//这个map不是太好，后面可以优化掉
    private int currentSize = 0;
    private int replicNum = (RpcServer.getRpcServerId() + 1) % 3;
    private int blockNum = 0;


    @Override
    public Map<String, String> get(String key) {
        return null;
    }

    @Override
    public boolean put(String key, Map<String, String> map) {
        byte[] value = encode(key, map);
        //要发送给Replica,future异步发送
        Future<Boolean> future = threadPool.submit(() ->
        {
            try {
                RpcClientFactory.inform(replicNum, value);
                return true;
            } catch (IOException e) {
                //与replica连接出现异常，更换replica节点
                try {
                    replicNum = (RpcServer.getRpcServerId() + 2) % 3;
                    RpcClientFactory.inform(replicNum, value);
                    return true;
                } catch (IOException E) {
                    return false;//不做备份，接口只返回一个boolean值，感觉有点问题
                }
            }
        });

        MemTable currentTable = getCurrentTable();

        if(currentSize<Config.MAX_NUM){
            currentTable.put(key,value);
            currentSize++;
        }else{
            //要落盘
            currentTable.persist();
            //更换存储的table
            currentTable = getCurrentTable();
            currentSize = 0;
            currentTable.put(key,value);
        }

        try {
            //等待备份完成
            future.get();
        } catch (InterruptedException e1) {
            return true;//这里的return有待商榷
        } catch (ExecutionException e2) {
            return true;//这里的return有待商榷
        }

        return true;

    }

    @Override
    public boolean batchPut(Map<String, Map<String, String>> map) {
        return false;
    }

    @Override
    public byte[] process(byte[] bytes) {
        return new byte[0];
    }


    private byte[] encode(String key, Map<String, String> map) {
        //这边最好要改成自己编码，不追求极致效率的话用gson应该也没啥问题，开发方便
        tmpMap.put(key,map);
        String json = gson.toJson(tmpMap);
        tmpMap.clear();
        return json.getBytes();
    }

    private MemTable getCurrentTable() {
        if (first.getState().equals(TableState.free)) {
            return first;
        } else if (second.getState().equals(TableState.free)) {
            return second;
        } else {
            //没有可用的memTable，等待落盘结束，并增大阻塞计数
            blockNum++;

            MemTable  perTable= getCurrentPerTable();
            try {
                //等待落盘结束
                perTable.getTask().get();
            } catch (InterruptedException e1) {
                return null;//这里的return有待商榷
            } catch (ExecutionException e2) {
                return null;//这里的return有待商榷
            }
            //阻塞次数过多，说明容量太小，扩容
            if (blockNum > Config.MAX_BLOCK_NUM) {
                Config.expasionMaxNum();
            }

            return perTable;
        }
    }

    private MemTable getCurrentPerTable() {
        if (first.getState().equals(TableState.in_per)) {
            return first;
        } else if (second.getState().equals(TableState.in_per)) {
            return second;
        } else {
            return null;
        }
    }



    public static void main(String[] args){
//        Gson gson = new Gson();
//        HashMap<String,String> maps = new HashMap<>();
//        HashMap<String,Map<String,String>> table = new HashMap<>();
//        JsonObject jsonObject = new JsonObject();
//        JsonElement jsonElement = new JsonObject()
//        maps.put("a","b");
//        maps.put("c","d");
//        table.put("a",maps);
//
//        for(int i =0;i<100000000;i++)
//            gson.toJson(table);
    }
}

