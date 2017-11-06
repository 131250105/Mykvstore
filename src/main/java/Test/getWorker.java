package Test;

import org.codehaus.jettison.json.JSONObject;

import java.util.HashMap;

/**
 * Created by H77 on 2017/11/5.
 */
public class getWorker implements Runnable{
    int num;
    Client client;
    String[] urls;
    public getWorker(int num) {
        this.num = num;
        client = new Client();
        urls = new String[]{
                "http://localhost:8500/process", "http://localhost:8501/process","http://localhost:8502/process"
        };
    }

    public void run() {
        for (int i = 0 ; i < 512 ;i+=15) {
            JSONObject js = new JSONObject();
            try {
                int tag = num*512+i;
                String key = "key"+tag;
                String result = client.httpGet(key);
                if(result == null || result.equals("find nothing")){
                    System.out.println(key);
                    System.out.println(result);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args){
        for(int i = 0 ; i < 6 ;i++){
            Thread r = new Thread(new getWorker(i));
            r.start();
        }
        System.out.println("end");
    }

}
