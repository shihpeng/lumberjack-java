package org.lumberjack;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.zip.Deflater;

public class FrameComposer {

    public final static String PROTOCOL_VERSION = "1";
    public final static String DATA_FRAME_TYPE = "D";
    public final static String COMPRESS_FRAME_TYPE = "C";
    public final static String WRITE_FRAME_TYPE = "W";

    public static byte[] createDataFrame(Map<String, String> dataMap, int sequenceNumber) throws IOException {
        ByteArrayOutputStream bytesOutput = new ByteArrayOutputStream();

        bytesOutput.write(PROTOCOL_VERSION.getBytes());
        bytesOutput.write(DATA_FRAME_TYPE.getBytes());
        bytesOutput.write(ByteBuffer.allocate(4).putInt(sequenceNumber).array());
        bytesOutput.write(ByteBuffer.allocate(4).putInt(dataMap.size()).array());

        for (Map.Entry<String, String> entry : dataMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            bytesOutput.write(ByteBuffer.allocate(4).putInt(key.length()).array());
            bytesOutput.write(key.getBytes());
            bytesOutput.write(ByteBuffer.allocate(4).putInt(value.getBytes().length).array());
            bytesOutput.write(value.getBytes());
        }

        return bytesOutput.toByteArray();
    }

    public static byte[] createCompressFrame(byte[][] dataFrames) throws IOException {
        ByteArrayOutputStream bytesOutput = new ByteArrayOutputStream();
        for (byte[] frame : dataFrames) {
            bytesOutput.write(frame);
        }
        byte[] toBeCompressed = bytesOutput.toByteArray();
        bytesOutput.reset();

        byte[] compressed = compressData(toBeCompressed);

        bytesOutput.write(PROTOCOL_VERSION.getBytes());
        bytesOutput.write(COMPRESS_FRAME_TYPE.getBytes());
        bytesOutput.write(ByteBuffer.allocate(4).putInt(compressed.length).array());
        bytesOutput.write(Arrays.copyOf(compressed, compressed.length));

        return bytesOutput.toByteArray();
    }

    private static byte[] compressData(final byte[] data){
        byte[] output = new byte[5120];

        Deflater deflater = new Deflater(6);
        deflater.setInput(data);
        deflater.finish();

        int compressedLength = deflater.deflate(output);
        deflater.end();

        return Arrays.copyOf(output, compressedLength);
    }
}
