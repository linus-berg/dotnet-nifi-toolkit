# Golden fixtures

These files were produced by Apache NiFi's own Java serializers (2.12.0), so
the tests check NifiKit against NiFi's real behaviour rather than against itself:

| File | Produced by |
|---|---|
| `nifi_v3_two_flowfiles.bin` | `org.apache.nifi.util.FlowFilePackagerV3` (two FlowFiles) |
| `nifi_s2s_two_packets.bin` | `org.apache.nifi.remote.codec.StandardFlowFileCodec` (the same two FlowFiles) |
| `nifi_v1.tar` | `org.apache.nifi.util.FlowFilePackagerV1` |

The CRC32 of `nifi_s2s_two_packets.bin` computed by `java.util.zip.CRC32` is
`2364656160`.

To regenerate, download `nifi-flowfile-packager`, `nifi-site-to-site-client`,
`nifi-utils`, `nifi-api`, `commons-compress`, `commons-text`, `commons-lang3`
and `commons-io` from Maven Central into a directory and run:

```bash
java -cp "<jar-dir>/*" GenFixtures.java <output-dir>
```

The attribute values in `GenFixtures.java` must stay in sync with
`TestData.cs`.
