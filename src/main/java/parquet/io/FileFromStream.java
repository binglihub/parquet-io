package parquet.io;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.io.InputStream;

public class FileFromStream implements InputFile {

    private SeekableInputStreamBuilder input;

    public FileFromStream(InputStream input){
        try {
            this.input = new SeekableInputStreamBuilder(input);
        } catch (IOException e){
            System.out.println(e);
        }
    }

    @Override
    public long getLength() throws IOException {
        return input.getLength();
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return input;
    }
}
