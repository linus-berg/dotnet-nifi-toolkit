import org.apache.nifi.util.*;
import org.apache.nifi.remote.codec.StandardFlowFileCodec;
import org.apache.nifi.remote.util.StandardDataPacket;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.zip.CRC32;

// Generates golden fixtures with NiFi's own serializers (attribute order fixed via LinkedHashMap).
public class GenFixtures {
  static Map<String, String> attrsA() {
    Map<String, String> m = new LinkedHashMap<>();
    m.put("filename", "a.txt");
    m.put("unicode", "åäö ✓ 😀");
    m.put("empty", "");
    m.put("big", "x".repeat(70_000));
    return m;
  }
  static Map<String, String> attrsB() {
    Map<String, String> m = new LinkedHashMap<>();
    m.put("filename", "b.bin");
    m.put("mime.type", "application/octet-stream");
    return m;
  }
  static byte[] contentB() { byte[] b = new byte[5_000]; for (int i = 0; i < b.length; i++) b[i] = (byte) (i * 31 + 7); return b; }

  public static void main(String[] a) throws Exception {
    Path dir = Path.of(a[0]);
    byte[] contentA = "hello world".getBytes(StandardCharsets.UTF_8);

    // FlowFile V3: two flowfiles back to back
    ByteArrayOutputStream v3 = new ByteArrayOutputStream();
    new FlowFilePackagerV3().packageFlowFile(new ByteArrayInputStream(contentA), v3, attrsA(), contentA.length);
    new FlowFilePackagerV3().packageFlowFile(new ByteArrayInputStream(contentB()), v3, attrsB(), contentB().length);
    Files.write(dir.resolve("nifi_v3_two_flowfiles.bin"), v3.toByteArray());

    // Site-to-Site StandardFlowFileCodec: same two packets
    ByteArrayOutputStream s2s = new ByteArrayOutputStream();
    StandardFlowFileCodec codec = new StandardFlowFileCodec();
    codec.encode(new StandardDataPacket(attrsA(), new ByteArrayInputStream(contentA), contentA.length), s2s);
    codec.encode(new StandardDataPacket(attrsB(), new ByteArrayInputStream(contentB()), contentB().length), s2s);
    Files.write(dir.resolve("nifi_s2s_two_packets.bin"), s2s.toByteArray());
    CRC32 crc = new CRC32(); crc.update(s2s.toByteArray());
    System.out.println("S2S CRC32 (java.util.zip.CRC32) = " + crc.getValue());

    // FlowFile V1: extract NiFi's attributes XML for special characters
    Map<String, String> v1 = new LinkedHashMap<>();
    v1.put("filename", "d.txt");
    v1.put("xml<&>\"'", "a<b & c>d \"q\" 'a'");
    v1.put("unicode", "åäö ✓");
    v1.put("multi", "line1\nline2\ttab");
    ByteArrayOutputStream v1out = new ByteArrayOutputStream();
    new FlowFilePackagerV1().packageFlowFile(new ByteArrayInputStream("v1 content".getBytes()), v1out, v1, 10);
    Files.write(dir.resolve("nifi_v1.tar"), v1out.toByteArray());
    System.out.println("wrote fixtures to " + dir);
  }
}
