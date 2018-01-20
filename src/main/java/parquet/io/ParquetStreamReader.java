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
package org.apache.parquet.hadoop;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.format.converter.ParquetMetadataConverter.MetadataFilter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.FileFromStream;

/**
 * Internal implementation of the Parquet file reader as a block container
 *
 * @author Julien Le Dem
 *
 */
public class ParquetStreamReader extends ParquetFileReader {

    /**
     * @param conf the Hadoop Configuration
     * @param file Path to a parquet file
     * @param footer a {@link ParquetMetadata} footer already read from the file
     * @throws IOException if the file can not be opened
     */
    public ParquetStreamReader(Configuration conf, Path file, ParquetMetadata footer) throws IOException {
        super(conf,file,footer);
    }

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


}
