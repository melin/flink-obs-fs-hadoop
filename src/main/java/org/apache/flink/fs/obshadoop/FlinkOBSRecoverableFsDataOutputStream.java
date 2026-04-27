/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.fs.obshadoop;

import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.obs.OBSFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

public class FlinkOBSRecoverableFsDataOutputStream extends RecoverableFsDataOutputStream {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkOBSRecoverableFsDataOutputStream.class);
    private final FileSystem fs;

    private final Path finalFile;

    private final Path stageFile;

    private final FSDataOutputStream outputStream;

    FlinkOBSRecoverableFsDataOutputStream(FileSystem fs, Path finalPath, Path stagePath) throws IOException {
        this.fs = Preconditions.checkNotNull(fs);
        this.finalFile = Preconditions.checkNotNull(finalPath);
        this.stageFile = Preconditions.checkNotNull(stagePath);
        this.outputStream = fs.create(stageFile);
    }

    FlinkOBSRecoverableFsDataOutputStream(
            final FileSystem fs, final FlinkOBSFsRecoverable recoverable) throws IOException {
        this.fs = Preconditions.checkNotNull(fs);
        this.finalFile = Preconditions.checkNotNull(recoverable.finalFile());
        this.stageFile = Preconditions.checkNotNull(recoverable.stageFile());

        safelyTruncateFile((OBSFileSystem) fs, stageFile, recoverable);

        outputStream = fs.append(stageFile);

        long pos = outputStream.getPos();
        if (pos != recoverable.offset()) {
            IOUtils.closeQuietly(outputStream);
            throw new IOException(
                    "Truncate failed: "
                            + stageFile
                            + " (requested="
                            + recoverable.offset()
                            + " ,size="
                            + pos
                            + ')');
        }
    }

    static class OBSFsCommitter implements Committer {
        private final FileSystem fs;

        private final FlinkOBSFsRecoverable recoverable;

        OBSFsCommitter(final FileSystem fs, final FlinkOBSFsRecoverable recoverable) {
            this.fs = Preconditions.checkNotNull(fs);
            this.recoverable = Preconditions.checkNotNull(recoverable);
        }

        @Override
        public void commit() throws IOException {
            LOG.info("start commit");

            final Path stagePath = recoverable.stageFile();
            final Path finalPath = recoverable.finalFile();
            final long expect = recoverable.offset();

            final FileStatus srcStatus;
            try {
                srcStatus = fs.getFileStatus(stagePath);
            } catch (IOException e) {
                LOG.error("Cannot commit: Staging file does not exist. Path:{}", stagePath);
                throw new IOException("Cannot commit: Staging file does not exist.");
            }

            if (srcStatus.getLen() != expect) {
                LOG.error("Cannot commit: File has trailing junk data. srcStatusLen:{}, expect:{}", srcStatus.getLen(), expect);
                throw new IOException("Cannot commit: File has trailing junk data.");
            }

            try {
                fs.rename(stagePath, finalPath);
            } catch (IOException e) {
                LOG.error("Commit file by rename failed: {} to {}", stagePath, finalPath);
                throw new IOException("Commit file by rename failed: " + stagePath + " to " + finalPath, e);
            }
        }

        @Override
        public void commitAfterRecovery() throws IOException {
            final Path stagePath = recoverable.stageFile();
            final Path finalPath = recoverable.finalFile();
            final long expect = recoverable.offset();

            FileStatus stageFileStatus = null;
            try {
                stageFileStatus = fs.getFileStatus(stagePath);
            } catch (FileNotFoundException e) {
                LOG.info("file not found");
            } catch (IOException e) {
                LOG.error("Commit during recovery failed: Could not access status of stage file.");
                throw new IOException("Commit during recovery failed: Could not access status of stage file.");
            }

            if (stageFileStatus != null) {
                if (stageFileStatus.getLen() > expect) {
                    safelyTruncateFile((OBSFileSystem) fs, stagePath, recoverable);
                }

                try {
                    fs.rename(stagePath, finalPath);
                } catch (IOException e) {
                    LOG.error("Commit file by rename failed: {} to {}", stagePath, finalPath);
                    throw new IOException("Commit file by rename failed: " + stagePath + " to " + finalPath, e);
                }
            }
            LOG.info("success commit");
        }

        @Override
        public RecoverableWriter.CommitRecoverable getRecoverable() {
            return recoverable;
        }
    }

    @Override
    public void flush() throws IOException {
        outputStream.hflush();
    }

    @Override
    public void sync() throws IOException {
        outputStream.hflush();
        outputStream.hsync();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        outputStream.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public RecoverableWriter.ResumeRecoverable persist() throws IOException {
        sync();
        return new FlinkOBSFsRecoverable(finalFile, stageFile, getPos());
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }

    @Override
    public Committer closeForCommit() throws IOException {
        final long position = getPos();
        close();
        return new FlinkOBSRecoverableFsDataOutputStream.OBSFsCommitter(
                fs, new FlinkOBSFsRecoverable(finalFile, stageFile, position));
    }

    @Override
    public long getPos() throws IOException {
        return outputStream.getPos();
    }

    private static void safelyTruncateFile(
            final OBSFileSystem fileSystem,
            final Path path,
            final FlinkOBSFsRecoverable recoverable)
            throws IOException {

        try {
            fileSystem.truncate(path, recoverable.offset());
        } catch (IOException e) {
            LOG.error("truncate failed,path:{},offset:{}", path, recoverable.offset());
            throw new IOException("truncate failed,path:" + path + "offset:" + recoverable.offset(), e);
        }
    }
}
