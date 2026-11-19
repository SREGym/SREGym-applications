// Regional MQTT ingress.
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public final class Gateway {
    private static final Logger LOG = Logger.getLogger("gateway");
    private static final ObjectMapper JSON = new ObjectMapper();

    static byte[] deviceKey(byte[] payload) throws Exception {
        JsonNode reading = JSON.readTree(payload);
        JsonNode device = reading == null ? null : reading.get("device_id");
        if (device == null || !device.isTextual() || device.textValue().isBlank()) {
            throw new IllegalArgumentException("device_id must be a nonempty string");
        }
        return device.textValue().getBytes(StandardCharsets.UTF_8);
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? fallback : value;
    }

    public static void main(String[] args) throws Exception {
        String clientId = env("CLIENT_ID", "eu-gateway");
        String topic = env("RAW_TOPIC", "raw-telemetry");
        String subscription = env("SUBSCRIPTION", "telemetry/#");
        String broker = "tcp://" + env("MQTT_HOST", "emqx") + ":" + env("MQTT_PORT", "1884");

        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_BOOTSTRAP"));
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "gateway-" + clientId);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);
        KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());

        MqttAsyncClient client = new MqttAsyncClient(broker, clientId, new MemoryPersistence());
        client.setManualAcks(true);
        AtomicLong forwarded = new AtomicLong();
        client.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String uri) {
                LOG.info("connected reconnect=" + reconnect + " client_id=" + clientId);
                try {
                    // Keep the callback free to process incoming deliveries.
                    client.subscribe(subscription, 1, null, new IMqttActionListener() {
                        @Override
                        public void onSuccess(IMqttToken token) {
                        }

                        @Override
                        public void onFailure(IMqttToken token, Throwable error) {
                            LOG.severe("subscribe failed: " + error);
                            System.exit(1);
                        }
                    });
                } catch (Exception error) {
                    LOG.severe("subscribe failed: " + error);
                    System.exit(1);
                }
            }

            @Override
            public void connectionLost(Throwable cause) {
                LOG.warning("connection lost: " + cause);
            }

            @Override
            public void messageArrived(String from, MqttMessage message) throws Exception {
                byte[] payload = message.getPayload();
                byte[] key = deviceKey(payload);
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key, payload);
                // Assign the forwarding identifier.
                String delivery = UUID.randomUUID().toString();
                record.headers().add("delivery_id", delivery.getBytes(StandardCharsets.UTF_8));
                int id = message.getId();
                producer.send(record, (metadata, error) -> {
                    if (error != null) {
                        LOG.severe("kafka send failed, delivery left unacknowledged: " + error);
                        System.exit(1);
                    }
                    try {
                        client.messageArrivedComplete(id, 1);
                    } catch (Exception ackError) {
                        LOG.warning("acknowledgement failed: " + ackError);
                    }
                    if (forwarded.incrementAndGet() % 1000 == 0) {
                        LOG.info("forwarded " + forwarded.get());
                    }
                });
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }
        });
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);
        options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
        options.setKeepAliveInterval(30);
        options.setAutomaticReconnect(true);
        options.setMaxInflight(32);
        client.connect(options).waitForCompletion();
        while (true) {
            Thread.sleep(5000);
        }
    }
}
