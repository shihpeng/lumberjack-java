package org.lumberjack;

import javax.net.ssl.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

/*
 *  This is a simple implementation of the Lumberjack client.
 *  GitHub: https://github.com/elasticsearch/logstash-forwarder
 *  Lumberjack protocol page: https://github.com/elasticsearch/logstash-forwarder/blob/master/PROTOCOL.md
 */
public class Client {

    public final static int SEQUENCE_MAX = (int) Math.pow(2, (Integer.SIZE*8 - 2)) -1;

    public final static String PROTOCOL_VERSION = "1";
    public final static String DATA_FRAME_TYPE = "D";
    public final static String COMPRESS_FRAME_TYPE = "C";
    public final static String WRITE_FRAME_TYPE = "W";

    private int sequence = 0;
    private int lastAck = 0;
    private int windowSize = 5000;

    private SSLSocket socket;
    private BufferedOutputStream out;
    private BufferedInputStream in;

    // a TrustManager that accepts any certificate
    private final static TrustManager[] DUMMY_TRUST_MANAGER = new TrustManager[]{new X509TrustManager()
    {
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {  return null; }
        public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
        public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
    }};

    public Client(String host, int port) throws GeneralSecurityException, IOException {
        this(host, port, null, -1);
    }

    public Client(String host, int port, String sslCertificate, int windowSize) throws GeneralSecurityException, IOException {

        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(null, DUMMY_TRUST_MANAGER, new SecureRandom());
        SSLSocketFactory factory = sslContext.getSocketFactory();

        socket = (SSLSocket) factory.createSocket(host, port);
        out = new BufferedOutputStream(socket.getOutputStream());
        in = new BufferedInputStream(socket.getInputStream());

        if(windowSize > 0){
            this.windowSize = windowSize;
        }
    }

    public void write(Map dataMap) throws IOException{

        if(sequence+1 > SEQUENCE_MAX){
            sequence = 0;
        }
        sequence += 1;

        byte[] dataFrame = FrameComposer.createDataFrame(dataMap, sequence);

        if((sequence - (lastAck + 1)) >= windowSize) {
            ack();
        }

        byte[] compressFrame = FrameComposer.createCompressFrame(new byte[][]{ dataFrame });
        out.write(compressFrame);
        out.flush();
    }

    private void ack() throws IOException{

        String version =  new String(new byte[]{(byte)in.read()});
        String type =  new String(new byte[]{(byte)in.read()});

        byte[] tmp = new byte[4];
        in.read(tmp, 0, 4);
        lastAck = ByteBuffer.wrap(tmp).getInt();

        if((sequence - (lastAck + 1)) >= windowSize){
            ack();
        }
    }

    private void close() throws IOException{
        if(socket != null){
            socket.close();
        }
    }

    public static void main(String[] args) {
        try {
            Client client = new Client("127.0.0.1", 5555, null, -1);

            Map<String, String> dataMap = new HashMap<String, String>();
            dataMap.put("line", "{\"key1\": \"value1\"}");
            dataMap.put("another_field", "data.of.another.field");

            client.write(dataMap);

            client.close();
        }
        catch(Exception ex){
            ex.printStackTrace();
            System.exit(1);
        }
    }
}