package parquet.io;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class StreamInputFile implements InputFile {

    private final String input;

    public StreamInputFile(String str){
        this.input = str;
    }

    @Override
    public long getLength() throws IOException {
        return input.getBytes().length;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return null;
    }
}
