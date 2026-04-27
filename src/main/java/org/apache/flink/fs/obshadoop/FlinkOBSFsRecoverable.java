package org.apache.flink.fs.obshadoop;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.core.fs.RecoverableWriter.CommitRecoverable;
import org.apache.flink.core.fs.RecoverableWriter.ResumeRecoverable;
import org.apache.hadoop.fs.Path;

/**
 * An implementation of the resume and commit descriptor objects for Hadoop's file system
 * abstraction.
 */
class FlinkOBSFsRecoverable implements CommitRecoverable, ResumeRecoverable {

    private final Path finalFile;

    private final Path stageFile;

    private final long offset;

    FlinkOBSFsRecoverable(Path targetFile, Path tempFile, long offset) {
        checkArgument(offset >= 0, "offset must be >= 0");
        this.finalFile = checkNotNull(targetFile, "finalFile");
        this.stageFile = checkNotNull(tempFile, "stageFile");
        this.offset = offset;
    }

    public Path finalFile() {
        return finalFile;
    }

    public Path stageFile() {
        return stageFile;
    }

    public long offset() {
        return offset;
    }

    @Override
    public String toString() {
        return "FlinkOBSFsRecoverable " + stageFile + " @ " + offset + " -> " + finalFile;
    }
}
