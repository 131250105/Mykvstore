import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * Created by H77 on 2017/10/13.
 */
public class IndexHashTable {

    private int keyLen;
    private byte[] contents;
    private int size;
    private int slotSize;
    private String tag;
    public IndexHashTable(int keyLen, byte[] contents ,int slotSize) {
        this.keyLen = keyLen;
        this.contents = contents;
        this.size = contents.length;
        this.slotSize = slotSize;
        byte[] tags = new byte[keyLen];
        this.tag = new String(tags);
    }

    public byte[] get(String key){
        int hash = hash(key);
        int step = hash1(key);
        int slot_position = (hash % (size/slotSize))*slotSize;
        byte[] _key = Arrays.copyOfRange(contents,slot_position,slot_position+keyLen);
        String tempKey = new String(_key);
        while (!tempKey.equals(tag)){
            if(tempKey.equals(key)){
                byte[] result = Arrays.copyOfRange(contents,slot_position+keyLen,slot_position+slotSize);
                return result;
            }else{
                slot_position = slot_position + step*slotSize;
                slot_position = slot_position + slotSize > size ?  slot_position % size : slot_position;
                _key = Arrays.copyOfRange(contents,slot_position,slot_position+4);
                tempKey = new String(_key);
            }
        }
        //意味着没找到，如果存在一定先填充了第一个槽位
        return null;
    }

    public void put(String key , byte[] content){
        int hash = hash(key);
        int slot_position = (hash % (size/slotSize))*slotSize;
        int step = hash1(key);
        //开发地址法
        byte[] _key = Arrays.copyOfRange(contents,slot_position,slot_position+keyLen);
        String temp_key = new String(_key);
        while (!temp_key.equals(tag)){
            slot_position = slot_position + step*slotSize;
            slot_position = slot_position + slotSize > size ?  slot_position%size : slot_position;
            _key = Arrays.copyOfRange(contents,slot_position,slot_position+keyLen);
            temp_key = new String(_key);
        }
        //赋值  keyoffset形式，两个int型
        byte[] keyBytes = key.getBytes();
        for( int index = 0 ; index < keyLen ; index++)  contents[slot_position+index] = keyBytes[index];
        for( int index = 0 ; index < 8 ; index++)  contents[slot_position+keyLen+index] = content[index];
    }

    private int hash(String key){
        return key.hashCode();
    }

    private int hash1(String key){
        return 1;
    }

}
