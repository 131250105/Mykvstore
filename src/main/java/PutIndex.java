import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/**
 * Created by zy812818
 * Created @ 2017/10/14.
 **/
public class PutIndex {

    private int length;

    private byte[] keyBytes;

    private long offset;

    private int valLength;

    public PutIndex(byte[] keyBytes,long offset,int valLength){
        this.keyBytes = keyBytes;
        this.length = keyBytes.length+8+4;
        this.offset = offset;
        this.valLength = valLength;
    }

    public void setOffset(long base){
        this.offset+=base;
    }

    public int toByteArray(byte[] bytes,int arrayOffset){
        bytes[arrayOffset++] = (byte)(1+keyBytes.length+8+4);
        System.arraycopy(keyBytes,0,bytes,arrayOffset,keyBytes.length);
        arrayOffset+=keyBytes.length;
        System.arraycopy(Util.longToBytes(offset),0,bytes,arrayOffset,8);
        arrayOffset+=8;
        System.arraycopy(Util.intToByteArray(valLength),0,bytes,arrayOffset,4);
        arrayOffset+=4;
        return arrayOffset;
    }
}
