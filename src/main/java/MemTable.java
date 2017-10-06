import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by zy812818
 * Created @ 2017/10/4.
 **/
public class MemTable {

    private TreeMap<String,byte[]> memTable = new TreeMap<>();

    private TableState state = TableState.free;

    private ExecutorService threadPool = Executors.newSingleThreadExecutor();

    private Future<Boolean> task;

    public void put(String key,byte[] value) {
        memTable.put(key,value);
    }

    public void persist(){
        this.state = TableState.in_per;
        //落盘
        this.task = threadPool.submit(()->
        {
            try {

                //上面是写入过程
                this.state = TableState.free;
                this.clear();
                return true;
            }catch (Exception e){
                return false;
            }
        });
    }

    public synchronized TableState getState() {
        return state;
    }

    public synchronized void setState(TableState state) {
        this.state = state;
    }

    public Future<Boolean> getTask() {
        return task;
    }

    public void clear(){
        this.memTable.clear();
    }
}
