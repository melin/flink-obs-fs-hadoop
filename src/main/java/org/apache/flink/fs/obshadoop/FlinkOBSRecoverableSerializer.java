package org.apache.flink.fs.obshadoop;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * serializer for the {@link FlinkOBSFsRecoverable}.
 */
@Internal
class FlinkOBSRecoverableSerializer implements SimpleVersionedSerializer<FlinkOBSFsRecoverable> {

    static final FlinkOBSRecoverableSerializer INSTANCE = new FlinkOBSRecoverableSerializer();

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    private static final int MAGIC_NUMBER = 0xd6434c5d;

    private FlinkOBSRecoverableSerializer() {
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(FlinkOBSFsRecoverable obj) throws IOException {
        final byte[] finalFileBytes = obj.finalFile().toString().getBytes(CHARSET);
        final byte[] stageFileBytes = obj.stageFile().toString().getBytes(CHARSET);
        final byte[] targetBytes = new byte[20 + finalFileBytes.length + stageFileBytes.length];

        ByteBuffer buf = ByteBuffer.wrap(targetBytes).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(MAGIC_NUMBER);
        buf.putLong(obj.offset());
        buf.putInt(finalFileBytes.length);
        buf.putInt(stageFileBytes.length);
        buf.put(finalFileBytes);
        buf.put(stageFileBytes);

        return targetBytes;
    }

    @Override
    public FlinkOBSFsRecoverable deserialize(int version, byte[] serialized) throws IOException {
        if (version == 1) {
            return deserialize(serialized);
        } else {
            throw new IOException("Unrecognized version or corrupt state: " + version);
        }
    }

    private static FlinkOBSFsRecoverable deserialize(byte[] serialized) throws IOException {
        final ByteBuffer buf = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

        if (buf.getInt() != MAGIC_NUMBER) {
            throw new IOException("Corrupt data: Unexpected magic number.");
        }

        final long offset = buf.getLong();
        final byte[] finalFileBytes = new byte[buf.getInt()];
        final byte[] stageFileBytes = new byte[buf.getInt()];
        buf.get(finalFileBytes);
        buf.get(stageFileBytes);

        final String finalPath = new String(finalFileBytes, CHARSET);
        final String stagePath = new String(stageFileBytes, CHARSET);

        return new FlinkOBSFsRecoverable(new Path(finalPath), new Path(stagePath), offset);
    }
}
