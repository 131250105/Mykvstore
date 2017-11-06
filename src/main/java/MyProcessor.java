/**
 * Created by zy812818
 * Created @ 2017/9/27.
 **/

import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.processor.Processor;
import cn.helium.kvstore.rpc.RpcClientFactory;
import cn.helium.kvstore.rpc.RpcServer;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

public class MyProcessor implements Processor {

    public MyProcessor() {
//        this.replica = new ReplicaTable();
//        this.run = new HeartBeatThread();
//        this.replicas = new LinkedList<>();
//        replicas.offer(replica);
//        run.setReplicas(replicas);
//        run.setProcessor(this);
//        Thread heartBeatThread = new Thread(run);
//        heartBeatThread.start();
        buildIndex();
    }
    //暂时不路由了，收到直接解决
    //还没写oplog
    private ExecutorService threadPool = Executors.newCachedThreadPool();
    private Gson gson = new Gson();
    private HashMap<Integer, MemTable> first = new HashMap<>();
    private HashMap<Integer, MemTable> second = new HashMap<>();
    private ReplicaTable replica;
    private HeartBeatThread run;
    private Queue<ReplicaTable> replicas;
    private int currentSize = 0;
    private int replicNum = (RpcServer.getRpcServerId() + 1) % 3;
    private boolean isFull = false;
    private byte[] serverList = new byte[]{0,1,2};
    private String processId;
    //    private HashMap<String,Map<String,String>> tmpMap = new HashMap<>();
    /*-----------------在读过程中会用到的结构 ------------------- */

    //对应最后一块缓存未落盘的key value信息 这部分信息 需要进程件的同步
    private HashMap<String,byte[]> rowMaps = new HashMap<>();

    //len_KVNum 对应一张索引表
    private ConcurrentHashMap<String, HashMap<String, byte[]>> indexTables = new ConcurrentHashMap<>();
    //len_KVNUM 对应一个路径
    private HashMap<String, String> globalDataFiles = new HashMap<>();
    private HashMap<String, String> globalIndexFiles = new HashMap<>();

    private ByteBuffer encodeBuffer = ByteBuffer.allocate(1024);
//    private ByteBuffer packet = ByteBuffer.allocate(40);
    public  Map<String, String> get(String key) {
        //调用get接口的时候 首先查看是否缓存了这条记录
        //没有的情况下 才需要查看索引
        byte[] content = rowMaps.getOrDefault(key,null);
        if(content != null)  return decode(content);
        int num = KvStoreConfig.getServersNum();
        //如果该进程没找到key对应value 查索引
        int len = key.length();
        String globalId = len+"";
        byte[] indexData = null;
        HashMap<String,byte[]> indexes = indexTables.get(globalId);
        if(indexes!= null && (indexData = indexes.getOrDefault(key,null)) != null){
            long offset = Util.bytes2Long(indexData);
            int _len = Util.byte4ToInt(indexData,8);
            String datafile = globalDataFiles.get(globalId);
            try {
                content = MyTestHDFSConn.readByOffset(datafile, offset, _len);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return decode(content);
        }
        //如果索引没查到 再查相邻进程的
        for(int i = 0 ; i < num ; i++) {
            if(RpcServer.getRpcServerId() != i) {
                try {
                    byte[] results = RpcClientFactory.inform(i,key.getBytes());
                    if(results.length != 0) return decode(results);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        //三种情况都没查到 说明没有这条记录
        return null;

//        packet.clear();
//        packet.putInt(index);
//        packet.put(key.getBytes());
//        packet.flip();
//        byte[] info = new byte[packet.limit()];
//        packet.get(info,0,packet.limit());

//        //查索引
//        int len = key.length();
//        HashMap<String ,byte[]> indexTable = indexTables.get(len);
//        byte[] indexData = indexTable.get(key);
//        Map<String, String> result = new HashMap<>();
//        //对应没有查到索引的情况
//        if (indexData == null) {
//           return null;
//        }
//        //解析偏移和value的长度，去读相应内容
//        int offset = Util.byte4ToInt(indexData, 0);
//        int _len = Util.byte4ToInt(indexData, 4);
//        String datafile = globalDataFiles.get(len);
//        try {
//            content = MyTestHDFSConn.readByOffset(datafile, offset, _len);
//            result = decode(content);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return result;
    }

//    public boolean dealSelf(String key, byte[] value) {
//
////        Future<Boolean> future;
//
//        MemTable currentTable = getCurrentTable(key.length());
//        if (!currentTable.put(key.getBytes(), value)) {
//            isFull = true;
//        }
//        if (isFull) {
//            currentTable = getCurrentTable(key.length());
//            //在切换缓存的同时 切换log日志
//            KLog log = FileMap.KLogMap.get(RpcServer.getRpcServerId()+ "_" + key.length());
//            log.switchLog();
//            currentTable.put(key.getBytes(), value);
//        }
//
//
//
//            //要发送给Replica,future异步发送
//            //这里要写op log 可以异步 可以同步
////            future = threadPool.submit(() ->
////            {
////                byte[] message = new byte[key.length() + value.length + 1];
////                System.arraycopy(key.getBytes(), 0, message, 0, key.length());
////                message[key.length()] = -1;
////                System.arraycopy(value, 0, message, key.length() + 1, value.length);
////                try {
////                    if (isFull) {
////                        RpcClientFactory.inform(replicNum, new byte[]{0, Config.KV_NUM});
////                    }
////                    RpcClientFactory.inform(replicNum, message);
////                    return true;
////                } catch (IOException e) {
////                    //备份或者网络出问题，切换备份
////                    try {
////                        replicNum = (RpcServer.getRpcServerId() + 2) % 3;
////                        RpcClientFactory.inform(replicNum, message);
////                        return true;
////                    } catch (IOException E) {
////                        return false;//备份失败，接口只返回一个boolean值，感觉有点问题
////                    }
////                }
////            });
//
//
////
////        try {
////            //等待备份完成
////            future.get();
////        } catch (InterruptedException e1) {
////            return true;//这里的return有待商榷
////        } catch (ExecutionException e2) {
////            return true;//这里的return有待商榷
////        }
//
//        return true;
//    }


    //先做最粗暴的同步 看看是不是这个问题
    @Override
    public synchronized boolean put(String key, Map<String, String> map) {
        //日志暂时已这种格式记录，保证日志一定插入到了HDFS中 但是感觉怪怪的 似乎没有缓存的必要
        //日志保存在/log目录下命名格式为serverid_len_num num为日志递增的一个标记
        //在落盘的过程中会调用logs.switchLog 落盘完成后 调用logs.deleteLog 目的为了删除前面已经落盘完成的信息的日志
        byte[] value = encode(key, map);
        byte[] log = encode(key,value);
        int keyLength = key.length();
        String logId = RpcServer.getRpcServerId()+"_"+keyLength;
//        KLog logs = FileMap.getKLog(logId);
        KLog logs = null;
        if((logs = FileMap.KLogMap.getOrDefault(logId,null))== null){
            logs = new KLog(logId);
            FileMap.KLogMap.put(logId,logs);
        }
        //记log前首先要保证缓存能够写入
        //如果缓存不能写入，换缓存快、切换日志
        MemTable currentTable = getCurrentTable(key.length());
        if(!currentTable.isFull(value)) {
            logs.append(log);
            currentTable.put(key.getBytes(),value);
        }else{
            HDFSWriteObject object = currentTable.persist();
            logs.switchLog(object);
            logs.append(log);
            HDFSWriteThread hdfsWriteThread = null;
            if(FileMap.threads.get(keyLength) == null){
                hdfsWriteThread = new HDFSWriteThread(keyLength);
                hdfsWriteThread.addObject(object);
                Thread thread = new Thread(hdfsWriteThread);
                thread.start();
            }else{
                hdfsWriteThread = FileMap.threads.get(keyLength);
                hdfsWriteThread.addObject(object);
            }
//            logs.deleteLog();
            currentTable.put(key.getBytes(),value);
        }
        return true;
    }


    @Override
    public boolean batchPut(Map<String, Map<String, String>> map) {
        //这边写的不好
//        String json = gson.toJson(tmpMap);
        for (Map.Entry<String, Map<String, String>> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
        return true;
    }

    @Override
    public int count(Map<String, String> map) {
        return 0;
    }

    @Override
    public Map<Map<String, String>, Integer> groupBy(List<String> list) {
        return null;
    }

    @Override
    public byte[] process(byte[] bytes) {
        String key = new String(bytes);
        byte[] results = rowMaps.getOrDefault(key,new byte[0]);
        if (results.length != 0) return results;
        //内存读这个进程对应的索引
        int len = key.length();
        String globalId = len+"";
        byte[] indexData = null;
        HashMap<String,byte[]> indexes = indexTables.get(globalId);
        if(indexes!= null && (indexData = indexes.getOrDefault(key,null)) != null){
            long offset = Util.bytes2Long(indexData);
            int _len = Util.byte4ToInt(indexData,8);
            String datafile = globalDataFiles.get(globalId);
            try {
                results = MyTestHDFSConn.readByOffset(datafile, offset, _len);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return results;
        }
        //results 是new byte[0]的情况 即没读到信息
        return results;
    }

    //这个备份clear的策略是有问题的，但跑过作业得测试用例应该没问题
//    @Override
//    public byte[] process(byte[] bytes) {
//        if (bytes[0] == 0) {
//            //备份的内容开始落盘了
//            replica.setState(TableState.in_per);
////            this.run.setPerReplica(replica);
//            ReplicaTable memTable = new ReplicaTable();
//            replicas.offer(memTable);
//            this.replica = memTable;
////            this.run.setReplica(replica);
//        } else if (bytes[0] == 1) {
//            //备份的内容落盘完成,将备份完成的table出队
//            replicas.poll();
////            table.clear();
////            this.run.setPerReplica(null);
//        } else if (bytes[0] == 2) {
//            //心跳,什么都不做，return就好
//        }else if (bytes[0] == -1) {
//            //别的节点准备落盘
//            int len = Util.byte4ToInt(bytes,1);
//            MemTable first_table = first.getOrDefault(len,null);
//            MemTable second_table = second.getOrDefault(len,null);
//            if(first_table!=null ){
//                if(first_table.getState().equals(TableState.in_per)){
//                    return new byte[]{-1};
//                }
////                else{
////                    first_table.setLock(true);
////                }
//            }
//
//            if(second_table!=null){
//                if(second_table.getState().equals(TableState.in_per)){
//                    return new byte[]{-1};
//                }
////                else{
////                    second_table.setLock(true);
////                }
//            }
//            return new byte[]{0};
//
//        }
////        else if (bytes[0] == -2) {
////            //别的节点要开始落盘了
////            int len = Util.byte4ToInt(bytes,1);
////            MemTable first_table = first.getOrDefault(len,null);
////            MemTable second_table = second.getOrDefault(len,null);
////            if(first_table!=null){
////                if(first_table.)
////            }
////        } else if (bytes[0] ==-3) {
////            //别的节点落盘结束了
////            int len = Util.byte4ToInt(bytes,1);
////            MemTable first_table = first.getOrDefault(len,null);
////            MemTable second_table = second.getOrDefault(len,null);
////            if(first_table!=null){
////                first_table.setLock(false);
////            }
////            if(second_table!=null){
////                second_table.setLock(false);
////            }
////        }
//        else if(bytes[0] ==-2){
//            //通知落盘的过程失败了，由这个节点转发
//            int len = Util.byte4ToInt(bytes,2);
//            byte sender = bytes[1];
//            try{
//                return RpcClientFactory.inform(getRestServer(sender,Config.KV_NUM), Util.specialIntToByteArray(len, Util.specialIntToByteArray(len,new byte[]{-2})));
//            }catch (IOException e){
//                e.printStackTrace();//不做任何操作
//            }
//        }else{
//            //数据，直接备份
//            replica.add(bytes);
//        }
//
//        return new byte[0];
//    }

    public void buildIndex() {
        try {
            List<String> logs = LocalFSConn.listAll(Config.LOG);
            List<String> files = MyTestHDFSConn.listAll(Config.DIR);
            int size = logs.size();
            //表明第一种情况文件系统中还没有文件是put状态
            if (size == 0) return;
//            String[] fileName = logs.get(0).split("");
//            this.processId = fileName[fileName.length-1].split(Config.SPLIT)[0];
            this.processId = "1";
            for (String file : files) {
                //所有进程都应该建立完整的索引，即也需要读其它进程的数据
                String[] tags = file.split(Config.SPLIT);
                if(tags[2].equals(this.processId)) {
//                    String len = Integer.parseInt(tags[1]);
//                    String globalId = len + "_" + tags[2];
                    if (tags[0].contains("index")) {
                        globalIndexFiles.put(tags[1], file);
                    } else {
                        globalDataFiles.put(tags[1], file);
                    }
                }
            }
            //将未建索引但记录日志的记录，直接以key byte[]的形式保存在一张map表中
            for(String path : logs){
                parseLog(path);
            }

            //为每个len建立索引
            CountDownLatch countDownLatch = new CountDownLatch(globalDataFiles.size());

            for (String id : globalIndexFiles.keySet()) {
                HDFSReadThread readThread = new HDFSReadThread(countDownLatch, this.indexTables,id,this.globalIndexFiles);
                threadPool.execute(readThread);
//                Thread t =new Thread(readThread);
//                t.start();
            }
            countDownLatch.await();
//            boolean s = indexTables.get(7).containsKey("key39844");
            System.out.println("!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void parseLog(String path){

        try {
            byte[] results = LocalFSConn.readFile(path);
            int len = results.length;
            int index = 0;
            while (index < len){
                int total = Util.byte4ToInt(results,index);
                int key_len = Util.byte4ToInt(results,index+4);
                index = index + 8;
                byte[] keybytes = Arrays.copyOfRange(results,index,index+key_len);
                String key = new String(keybytes);
                byte[] value = Arrays.copyOfRange(results,index+key_len,index+total);
                rowMaps.put(key,value);
                index = index+total;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    private byte[] encode(String key, Map<String, String> map) {
        //这边最好要改成自己编码，不追求极致效率的话用gson应该也没啥问题，开发方便
//        tmpMap.put(key, map);
//        String json = gson.toJson(map);
//        tmpMap.clear();
        encodeBuffer.clear();
        Set<String> keys = map.keySet();
        for(String k : keys){
            byte[] key_bytes = k.getBytes();
            byte[] value_bytes = map.get(k).getBytes();
            byte key_len = (byte) key_bytes.length;
            byte value_len = (byte)value_bytes.length;
            encodeBuffer.put(key_len);
            encodeBuffer.put(value_len);
            encodeBuffer.put(key_bytes);
            encodeBuffer.put(value_bytes);
        }
        encodeBuffer.flip();
        int len = encodeBuffer.limit();
        byte[] contents = new byte[len];
        encodeBuffer.get(contents,0,len);
        return contents;
    }
    //为values添加头部
    public byte[] encode(String key , byte[] values){
        encodeBuffer.clear();
//        int len =  key.length()+values.length;
//        int key_len = key.length();
        int len =key.length()+values.length;
        int key_len = key.length();
        encodeBuffer.putInt(len);
        encodeBuffer.putInt(key_len);
        encodeBuffer.put(key.getBytes());
        encodeBuffer.put(values);
        encodeBuffer.flip();
        int length = encodeBuffer.limit();
        byte[] results = new byte[length];
        encodeBuffer.get(results,0,length);
        return results;
    }

    private synchronized Map<String, String> decode(byte[] content) {
//        Map<String, String> maps = gson.fromJson(new String(content), HashMap.class);
        Map<String,String> maps = new HashMap<>();
        encodeBuffer.clear();
        encodeBuffer.put(content);
        encodeBuffer.flip();
        int len = encodeBuffer.limit();
        int index = 0;
        while(index < len){
            int key_len = encodeBuffer.get() & 0XFF;
            int value_len = encodeBuffer.get() & 0XFF;
//            int key_len = encodeBuffer.getInt();
//            int value_len = encodeBuffer.getInt();
            byte[] key_bytes = new byte[key_len];
            byte[] value_bytes = new byte[value_len];
            encodeBuffer.get(key_bytes);
            encodeBuffer.get(value_bytes);
            String key = new String(key_bytes);
            String value = new String(value_bytes);
            maps.put(key,value);
            index = index+2+key_len+value_len;
        }
        return maps;
    }

    private synchronized MemTable getCurrentTable(int key) {

        MemTable table = first.getOrDefault(key, null);
        if (table == null) {
            table = new MemTable(key);
            first.put(key, table);
            return table;
        } else {
            if (table.getState().equals(TableState.free)) {
                return table;
            }else {
                table = second.getOrDefault(key, null);
                if (table == null) {
                    table = new MemTable(key);
                    second.put(key, first.get(key));
                    first.put(key,table);
                    return table;
                } else {
                    if (table.getState().equals(TableState.free)) {
                        second.put(key, first.get(key));
                        first.put(key,table);
                        table.setCurrentOffset(FileMap.offsets.get(key));
                        return table;
                    }else {
                        MemTable perTable = first.get(key);
                        try {
                            //等待落盘结束
                            perTable.getTask().get();
                        } catch (InterruptedException e1) {
                            return null;//这里的return有待商榷
                        } catch (ExecutionException e2) {
                            return null;//这里的return有待商榷
                        }
                        return perTable;
                    }
                }
            }
        }
    }

    private int getRestServer(byte byte1,byte byte2){
        for(int i =0;i<serverList.length;i++){
            if(serverList[i]!=byte1 && serverList[i]!=byte2)
                return serverList[i];
        }
        return -1;
    }


    public void addIndexTables(String Id , HashMap<String, byte[] >indexTable){
        this.indexTables.put(Id,indexTable);
    }

    public static void main(String[] args) {
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

//        byte a = 3;
//        byte b = 1;
//        System.out.println((a % 2 == b));
//

//        String key = "key";
//        String column = "aaa";
//        String value = "bbb";
//        MyProcessor pro = new MyProcessor();
//        long start = System.currentTimeMillis();
//        for(int i = 0 ; i < 1024 ;i++) {
//            HashMap<String, String> m = new HashMap<String, String>();
//            m.put(column+i, value);
//            m.put(column+i+1, "han");
//            pro.put(key+i, m);
//        }
//        long total = System.currentTimeMillis()-start;
//        System.out.println(total+"ms");

//        MyProcessor pro1 = new MyProcessor();
//        Map<String, String> result = pro1.get("key51");
//        System.out.print(".");
//        result = pro1.get("key5210");
//        System.out.print(".");
//        System.out.println("opt/logs/0_3_4".split("/")[0]);
        String[] fileName = "opt/logs/0_3_4".split("/");
        int processId = Integer.parseInt(fileName[fileName.length-1].split(Config.SPLIT)[0]);
        System.out.println(processId);
    }
}

