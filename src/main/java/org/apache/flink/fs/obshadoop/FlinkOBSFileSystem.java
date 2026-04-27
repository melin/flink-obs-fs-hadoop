package org.apache.flink.fs.obshadoop;

import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.hadoop.fs.FileSystem;

public class FlinkOBSFileSystem extends HadoopFileSystem {

    public FlinkOBSFileSystem(FileSystem obsFileSystem) {
        super(obsFileSystem);
    }

    @Override
    public RecoverableWriter createRecoverableWriter() {
        if (!"obs".equals(getHadoopFileSystem().getScheme())) {
            throw new UnsupportedOperationException(
                "This obs file system "
                    + "implementation does not support recoverable writers.");
        }

        return new FlinkOBSRecoverableWriter(getHadoopFileSystem());
    }

}
