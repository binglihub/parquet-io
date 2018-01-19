package parquet.io;

import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class SeekableInputStreamBuilder extends SeekableInputStream{

    private InputStream input;

    private long pos;

    private long mark = 0;

    private int count;

    public SeekableInputStreamBuilder(InputStream input) throws IOException {
        this.input = input;
        this.input.reset();
        this.pos = 0;
        this.count = input.available();
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public void seek(long newPos) throws IOException {
        if(newPos<0 || newPos>=count) throw new IOException("incorrect seek position: "+newPos);
        pos = newPos;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
        readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
        if(start+len>bytes.length) throw new IOException("Array out of bound. size of array: "
                + bytes.length+" start: "+start+" len: "+len);
        if(pos+len>count) throw new EOFException("Stream size: "+count+" pos: "+pos
                + " len: "+len);
        input.reset();
        input.skip(pos);
        for (int i = 0; i < len; i++) {
            bytes[start+i] = (byte)input.read();
            pos++;
        }
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
        input.reset();
        input.skip(pos);
        int c = input.read();
        if(c == -1) return -1;
        buf.put((byte)c);
        pos++;
        int size = 1;
        while(pos<count){
            buf.put((byte)input.read());
            pos++;
            size++;
        }
        return size;
    }

    @Override
    public void readFully(ByteBuffer buf) throws IOException {
        read(buf);
    }

    @Override
    public int read() throws IOException {
        input.reset();
        input.skip(pos);
        int result = input.read();
        pos++;
        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException{
        input.reset();
        input.skip(pos);
        int result = input.read(b,off,len);
        pos+=len;
        return result;
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

}
