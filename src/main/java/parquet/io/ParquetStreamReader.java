/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
//package org.apache.parquet.hadoop;

package parquet.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter.MetadataFilter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndian;
import static org.apache.parquet.hadoop.ParquetFileWriter.MAGIC;

/**
 * Internal implementation of the Parquet file reader as a block container
 *
 * @author Julien Le Dem
 *
 */
public class ParquetStreamReader {

    /**
     * Reads the meta data in the footer of the file.
     * Skipping row groups (or not) based on the provided filter
     * @param input the Parquet File stream
     * @param filter the filter to apply to row groups
     * @return the metadata with row groups filtered.
     * @throws IOException  if an error occurs while reading the file
     */
    public static ParquetMetadata readFooter(InputStream input, MetadataFilter filter) throws IOException {
        return readFooter(new FileFromStream(input), filter);
    }

    /**
     * Reads the meta data block in the footer of the file using provided input stream
     * @param file a {@link InputFile} to read
     * @param filter the filter to apply to row groups
     * @return the metadata blocks in the footer
     * @throws IOException if an error occurs while reading the file
     */
    public static final ParquetMetadata readFooter(
            InputFile file, MetadataFilter filter) throws IOException {
        ParquetMetadataConverter converter;
        // TODO: remove this temporary work-around.
        // this is necessary to pass the Configuration to ParquetMetadataConverter
        // and should be removed when there is a non-Hadoop configuration.
        if (file instanceof HadoopInputFile) {
            converter = new ParquetMetadataConverter(
                    ((HadoopInputFile) file).getConfiguration());
        } else {
            converter = new ParquetMetadataConverter();
        }
        SeekableInputStream in = file.newStream();
        try {
            return readFooter(converter, file.getLength(), file.toString(), in, filter);
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

    /**
     * Reads the meta data block in the footer of the file using provided input stream
     * @param fileLen length of the file
     * @param filePath file location
     * @param f input stream for the file
     * @param filter the filter to apply to row groups
     * @return the metadata blocks in the footer
     * @throws IOException if an error occurs while reading the file
     */
    private static final ParquetMetadata readFooter(ParquetMetadataConverter converter, long fileLen, String filePath, SeekableInputStream f, MetadataFilter filter) throws IOException {
        LOG.debug("File length {}", fileLen);
        int FOOTER_LENGTH_SIZE = 4;
        if (fileLen < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) { // MAGIC + data + footer + footerIndex + MAGIC
            throw new RuntimeException(filePath + " is not a Parquet file (too small)");
        }
        long footerLengthIndex = fileLen - FOOTER_LENGTH_SIZE - MAGIC.length;
        LOG.debug("reading footer index at {}", footerLengthIndex);

        f.seek(footerLengthIndex);
        int footerLength = readIntLittleEndian(f);
        byte[] magic = new byte[MAGIC.length];
        f.readFully(magic);
        if (!Arrays.equals(MAGIC, magic)) {
            throw new RuntimeException(filePath + " is not a Parquet file. expected magic number at tail " + Arrays.toString(MAGIC) + " but found " + Arrays.toString(magic));
        }
        long footerIndex = footerLengthIndex - footerLength;
        LOG.debug("read footer length: {}, footer index: {}", footerLength, footerIndex);
        if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
            throw new RuntimeException("corrupted file: the footer index is not within the file");
        }
        f.seek(footerIndex);
        return converter.readParquetMetadata(f, filter);
    }




    /**
     * @param input the Parquet File stream
     * @param footer a {@link ParquetMetadata} footer already read from the file
     * @throws IOException if the file can not be opened
     */
    public ParquetStreamReader(InputStream input, ParquetMetadata footer) throws IOException {
        this.f = new FileFromStream(input).newStream();
        this.footer = footer;
        this.fileMetaData = footer.getFileMetaData();
        this.blocks = footer.getBlocks();
        for (ColumnDescriptor col : footer.getFileMetaData().getSchema().getColumns()) {
            paths.put(ColumnPath.get(col.getPath()), col);
        }
    }


    private Map<ColumnPath, ColumnDescriptor> paths = new HashMap<ColumnPath, ColumnDescriptor>();

    private SeekableInputStream f;

    private FileMetaData fileMetaData; // may be null

    private ParquetMetadata footer;

    private List<BlockMetaData> blocks;

    private static final Logger LOG = LoggerFactory.getLogger(ParquetStreamReader.class);
}
