package Test;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.HashMap;

/**
 * Created by H77 on 2017/10/25.
 */
public class worker  implements Runnable{
    int num;
    Client client;
    String[] urls;
    public worker(int num){
        this.num = num;
        client = new Client();
        urls = new String[]{
//        "http://localhost:8500/process", "http://localhost:8501/process","http://localhost:8502/process"
        "http://192.168.0.101:8500/process", "http://192.168.0.104:8500/process","http://192.168.0.107:8500/process"
        };
    }

    public void run() {
        for (int i = 0 ; i < 2048 ;i++) {
            JSONObject js = new JSONObject();
                try {
                    int tag = num*2048+i;
                    js.put("key","key"+tag);
                    HashMap<String,String> maps = new HashMap<>();
                    maps.put("aaa"+tag,"bbb");
                    maps.put("bbb"+tag,"ccc");
                    js.put("value",maps);
                    System.out.println("Thread"+num+"Post"+i);
                    client.postForm(urls[i%3],js);
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }
    }


    public static void main(String[] args){
        for(int i =0 ; i < 5 ;i++){
            Thread r = new Thread(new worker(i));
            r.start();
        }
        System.out.println("end");
    }
}
