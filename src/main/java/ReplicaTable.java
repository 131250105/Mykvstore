import java.util.ArrayList;

/**
 * Created by zy812818
 * Created @ 2017/10/6.
 **/
public class ReplicaTable {

    private TableState state = TableState.free;

    private ArrayList<byte[]> table = new ArrayList<>(1000);

    public ArrayList<byte[]> getTable() {
        return table;
    }

    public void add(byte[] data){
        table.add(data);
    }

    public TableState getState() {
        return state;
    }

    public void setState(TableState state) {
        this.state = state;
    }
}
