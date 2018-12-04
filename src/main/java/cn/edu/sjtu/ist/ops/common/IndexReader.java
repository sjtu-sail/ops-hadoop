/*
 * Copyright 2018 SJTU IST Lab
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.sjtu.ist.ops.common;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.sjtu.ist.ops.util.OpsUtils;

public class IndexReader {
    private static final Logger logger = LoggerFactory.getLogger(IndexReader.class);
    public static final int MAP_OUTPUT_INDEX_RECORD_LENGTH = 24;

    private final File indexFile;
    private final int partitions;
    private final int size;
    private LongBuffer entries;

    public IndexReader(String indexFilePath) {
        this.indexFile = new File(indexFilePath);
        long length = this.indexFile.length();
        this.partitions = (int) length / MAP_OUTPUT_INDEX_RECORD_LENGTH;
        this.size = partitions * MAP_OUTPUT_INDEX_RECORD_LENGTH;

        try {
            ByteBuffer buf = ByteBuffer.allocate(size);
            DataInputStream dis = new DataInputStream(new FileInputStream(this.indexFile));
            OpsUtils.readFully(dis, buf.array(), 0, size);
            this.entries = buf.asLongBuffer();
            logger.debug("Index file: " + indexFilePath + " size: " + this.size + " partitions: " + this.partitions);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            // TODO: handle exception
        } catch (Exception e) {
            // TODO: handle exception
        }

    }

    public IndexRecord getIndex(int partition) {
        final int pos = partition * MAP_OUTPUT_INDEX_RECORD_LENGTH / 8;
        return new IndexRecord(entries.get(pos), entries.get(pos + 1), entries.get(pos + 2));
    }
}