import java.nio.charset.StandardCharsets;

public final class GatewayKeyTest {
    public static void main(String[] args) throws Exception {
        String[][] valid = {
            {"{\"device_id\":\"dev-eu-0000\",\"seq\":1}", "dev-eu-0000"},
            {"{ \"seq\": 1, \"device_id\" : \"dev-eu-0001\" }", "dev-eu-0001"},
            {"{\"device_id\":\"dev-\\\"quoted\\u002d雪\"}", "dev-\"quoted-雪"}
        };
        for (String[] example : valid) {
            String actual = new String(Gateway.deviceKey(example[0].getBytes(StandardCharsets.UTF_8)),
                                       StandardCharsets.UTF_8);
            if (!example[1].equals(actual)) throw new AssertionError(actual);
        }
        String[] invalid = {"{}", "null", "{\"device_id\":null}", "{\"device_id\":1}",
                            "{\"device_id\":\"\"}", "{\"device_id\":\"  \"}", "{"};
        for (String example : invalid) {
            try {
                Gateway.deviceKey(example.getBytes(StandardCharsets.UTF_8));
            } catch (Exception expected) {
                continue;
            }
            throw new AssertionError("accepted invalid device_id: " + example);
        }
        System.out.println("PASS: 3 valid encodings and 7 invalid payloads");
    }
}
