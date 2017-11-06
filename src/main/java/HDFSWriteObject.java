import java.io.RandomAccessFile;

/**
 * Created by zy812818
 * Created @ 2017/11/5.
 **/
public class HDFSWriteObject {


//    public void setRandomAccessFile(RandomAccessFile randomAccessFile) {
//        this.randomAccessFile = randomAccessFile;
//    }

    public void setPath(String path) {
        this.path = path;
    }

    public HDFSWriteObject(byte[] valueContent, byte[] indexContent) {
        this.valueContent = valueContent;
        this.indexContent = indexContent;

    }

    public byte[] getValueContent() {
        return valueContent;
    }

    public byte[] getIndexContent() {
        return indexContent;
    }

//    public RandomAccessFile getRandomAccessFile() {
//        return randomAccessFile;
//    }

    public String getPath() {
        return path;
    }

    private byte[] valueContent;

    private byte[] indexContent;

//    private RandomAccessFile randomAccessFile;

    private String path ;


}
