/**
 * Created by zy812818
 * Created @ 2017/9/27.
 **/

import cn.helium.kvstore.processor.Processor;

import java.util.Map;

public class MyProcessor implements Processor{

    //默认的无参构造函数应该也可以被反射调用

    @Override
    public Map<String, String> get(String s) {
        return null;
    }

    @Override
    public boolean put(String s, Map<String, String> map) {
        return false;
    }

    @Override
    public boolean batchPut(Map<String, Map<String, String>> map) {
        return false;
    }

    @Override
    public byte[] process(byte[] bytes) {
        return new byte[0];
    }
}

