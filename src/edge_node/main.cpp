#include <math.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#if defined(ARDUINO)
#define EDGE_HAS_ARDUINO_SDK 1
#elif defined(__has_include)
#if __has_include(<Arduino.h>) && __has_include(<WiFi.h>) && \
    __has_include(<PubSubClient.h>) && __has_include(<ArduinoJson.h>)
#define EDGE_HAS_ARDUINO_SDK 1
#endif
#endif

#ifdef EDGE_HAS_ARDUINO_SDK
#include <Arduino.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#else
static const uint8_t OUTPUT = 1;
static const uint8_t HIGH = 1;
static const uint8_t LOW = 0;
static const int WIFI_STA = 1;
static const int WL_CONNECTED = 3;

enum adc_attenuation_t { ADC_0db = 0, ADC_2_5db = 1, ADC_6db = 2, ADC_11db = 3 };

struct String {
    const char* c_str() const { return ""; }
};

struct IPAddress {
    String toString() const { return String(); }
};

struct HostSerial {
    void begin(unsigned long) {}
    void println(const char* = "") {}
    void printf(const char*, ...) {}
};
static HostSerial Serial;

struct HostEsp {
    uint64_t getEfuseMac() const { return 0ULL; }
};
static HostEsp ESP;

class WiFiClient {};

struct HostWiFi {
    void mode(int) {}
    void disconnect(bool = false, bool = false) {}
    int begin(const char*, const char* = nullptr, int32_t = 0,
              const uint8_t* = nullptr, bool = true) {
        return WL_CONNECTED;
    }
    int status() const { return 0; }
    IPAddress localIP() const { return IPAddress(); }
    int32_t RSSI() const { return 0; }
};
static HostWiFi WiFi;

class PubSubClient {
public:
    explicit PubSubClient(WiFiClient&) {}
    bool connected() { return false; }
    bool loop() { return true; }
    bool connect(const char*, const char* = nullptr, const char* = nullptr,
                 const char* = nullptr, uint8_t = 0, bool = false,
                 const char* = nullptr, bool = true) {
        return false;
    }
    bool publish(const char*, const char*, bool = false) { return false; }
    bool publish(const char*, const uint8_t*, unsigned int, bool = false) {
        return false;
    }
    int state() { return -1; }
    PubSubClient& setServer(const char*, uint16_t) { return *this; }
    bool setBufferSize(uint16_t) { return true; }
    PubSubClient& setKeepAlive(uint16_t) { return *this; }
    PubSubClient& setSocketTimeout(uint16_t) { return *this; }
};

struct HostJsonSerialized {
    const char* raw;
};

struct HostJsonVariant {
    HostJsonVariant operator[](const char*) { return HostJsonVariant(); }
    template <typename T>
    HostJsonVariant& operator=(const T&) {
        return *this;
    }
};

class JsonDocument {
public:
    HostJsonVariant operator[](const char*) { return HostJsonVariant(); }
};

inline HostJsonSerialized serialized(const char* value) {
    HostJsonSerialized out = {value};
    return out;
}
inline size_t measureJson(const JsonDocument&) { return 0U; }
inline size_t serializeJson(const JsonDocument&, char*, size_t) { return 0U; }
template <size_t N>
inline size_t serializeJson(const JsonDocument&, char (&)[N]) {
    return 0U;
}

inline uint32_t millis() { return 0U; }
inline void delay(uint32_t) {}
inline void pinMode(uint8_t, uint8_t) {}
inline void digitalWrite(uint8_t, uint8_t) {}
inline int analogRead(uint8_t) { return 0; }
inline void analogReadResolution(uint8_t) {}
inline void analogSetPinAttenuation(uint8_t, adc_attenuation_t) {}
#endif

static const char *WIFI_SSID = "Wokwi-GUEST";
static const char *WIFI_PASSWORD = "";
static const int32_t WIFI_CHANNEL = 6;

static const char *MQTT_HOST = "broker.hivemq.com";
static const uint16_t MQTT_PORT = 1883;

static const char *NODE_ID = "edge-node-01";
static const char *TOPIC_TELEMETRY = "eeair/cilan1m0/edge-node-01/telemetry";
static const char *TOPIC_STATUS = "eeair/cilan1m0/edge-node-01/status";

static const uint8_t PIN_SENSOR = 36;
static const uint8_t PIN_LED = 2;

static const double ADC_MAX_COUNTS = 4095.0;
static const double ADC_REF_VOLTS = 3.3;

static const uint32_t SAMPLE_PERIOD_MS = 100;
static const uint32_t HEARTBEAT_PERIOD_MS = 30000;
static const uint32_t RECONNECT_PERIOD_MS = 5000;
static const uint32_t ANOMALY_PULSE_MS = 200;
static const uint32_t BLINK_NO_WIFI_MS = 100;
static const uint32_t BLINK_NO_MQTT_MS = 500;
static const float DV_SURGE_VOLTS = 0.25f;
static const double RAIL_FAULT_LOW_V = 0.05;
static const double RAIL_FAULT_HIGH_V = 3.25;

static const bool LOG_NOMINAL_SAMPLES = true;

static const size_t WINDOW_SIZE = 50;
static const float Z_THRESHOLD = 2.5f;
static const double SIGMA_EPSILON = 0.0001;

static const bool GATED_BUFFER_UPDATE = true;
static const uint32_t REBASELINE_RUN = 10;

class RingBuffer {
public:
  RingBuffer() : head_(0), count_(0) {
    for (size_t i = 0; i < WINDOW_SIZE; ++i) {
      data_[i] = 0.0f;
    }
  }

  void push(float value) {
    data_[head_] = value;
    head_ = (head_ + 1) % WINDOW_SIZE;
    if (count_ < WINDOW_SIZE) {
      ++count_;
    }
  }

  size_t size() const { return count_; }
  bool isFull() const { return count_ == WINDOW_SIZE; }

  float at(size_t i) const {
    size_t oldest = (head_ + WINDOW_SIZE - count_) % WINDOW_SIZE;
    return data_[(oldest + i) % WINDOW_SIZE];
  }

private:
  float data_[WINDOW_SIZE];
  size_t head_;
  size_t count_;
};

struct WindowStats {
  double mean;
  double stdDev;
};

static WindowStats computeStats(const RingBuffer &window) {
  WindowStats stats = {0.0, 0.0};
  const size_t n = window.size();
  if (n == 0) {
    return stats;
  }
  double sum = 0.0;
  for (size_t i = 0; i < n; ++i) {
    sum += window.at(i);
  }
  stats.mean = sum / static_cast<double>(n);
  if (n < 2) {
    return stats;
  }
  double sumSq = 0.0;
  for (size_t i = 0; i < n; ++i) {
    const double d = window.at(i) - stats.mean;
    sumSq += d * d;
  }
  stats.stdDev = sqrt(sumSq / static_cast<double>(n - 1));
  return stats;
}

enum SampleState { STATE_WARMUP, STATE_NOMINAL, STATE_ANOMALY };

struct FilterDecision {
  SampleState state;
  double mean;
  double stdDev;
  double z;
  bool flatSignal;
};

static FilterDecision evaluateSample(const RingBuffer &window, float x, float zThreshold) {
  FilterDecision d = {STATE_WARMUP, 0.0, 0.0, 0.0, false};
  if (!window.isFull()) {
    return d;
  }
  const WindowStats stats = computeStats(window);
  d.mean = stats.mean;
  d.stdDev = stats.stdDev;
  if (stats.stdDev < SIGMA_EPSILON) {
    d.flatSignal = true;
    d.z = 0.0;
  } else {
    d.z = (static_cast<double>(x) - stats.mean) / stats.stdDev;
  }
  d.state = (fabs(d.z) >= zThreshold) ? STATE_ANOMALY : STATE_NOMINAL;
  return d;
}

static const char *stateName(SampleState s) {
  switch (s) {
    case STATE_WARMUP: return "WARMUP";
    case STATE_ANOMALY: return "ANOMALY";
    default: return "NOMINAL";
  }
}

static size_t buildTelemetryJson(char *out, size_t outSize, const char *nodeId,
                                 uint32_t seq, uint32_t tMs, uint16_t raw, double volts,
                                 const FilterDecision &d) {
  char vText[16];
  char muText[16];
  char sdText[16];
  char zText[24];
  snprintf(vText, sizeof(vText), "%.3f", volts);
  snprintf(muText, sizeof(muText), "%.3f", d.mean);
  snprintf(sdText, sizeof(sdText), "%.3f", d.stdDev);
  snprintf(zText, sizeof(zText), "%.2f", d.z);

  JsonDocument doc;
  doc["node"] = nodeId;
  doc["seq"] = seq;
  doc["t_ms"] = tMs;
  doc["raw"] = raw;
  doc["v"] = serialized(vText);
  doc["mu"] = serialized(muText);
  doc["sd"] = serialized(sdText);
  doc["z"] = serialized(zText);
  doc["state"] = stateName(d.state);

  if (measureJson(doc) >= outSize) {
    if (outSize > 0) {
      out[0] = '\0';
    }
    return 0;
  }
  return serializeJson(doc, out, outSize);
}

enum BufferAction {
  PUSH_WARMUP,
  PUSH_NOMINAL,
  PUSH_LEGACY,
  PUSH_REBASELINE,
  SKIP_ANOMALY
};

static BufferAction decideBufferAction(SampleState state, uint32_t &anomalyRun, bool gated) {
  if (state == STATE_WARMUP) {
    return PUSH_WARMUP;
  }
  if (state == STATE_NOMINAL) {
    anomalyRun = 0;
    return PUSH_NOMINAL;
  }
  ++anomalyRun;
  if (!gated) {
    return PUSH_LEGACY;
  }
  return (anomalyRun >= REBASELINE_RUN) ? PUSH_REBASELINE : SKIP_ANOMALY;
}

static WiFiClient wifiClient;
static PubSubClient mqtt(wifiClient);
static RingBuffer window;

static char mqttClientId[32];
static char telemetryPayload[256];
static char statusPayload[320];
static char lwtPayload[96];
static bool mqttAttempted = false;

static uint32_t sampleSeq = 0;
static uint32_t sentCount = 0;
static uint32_t warmupCount = 0;
static uint32_t anomalyCount = 0;
static uint32_t droppedCount = 0;
static uint32_t failedCount = 0;
static uint32_t flatCount = 0;
static uint32_t skippedCount = 0;
static uint32_t rebaselineCount = 0;
static uint32_t anomalyRun = 0;
static bool inFlatSignal = false;
static bool hasPrevVolts = false;
static float prevVolts = 0.0f;

static uint32_t lastSampleMs = 0;
static uint32_t lastHeartbeatMs = 0;
static uint32_t lastWifiAttemptMs = 0;
static uint32_t lastMqttAttemptMs = 0;
static uint32_t lastBlinkMs = 0;
static uint32_t ledPulseEndMs = 0;
static bool ledPulseActive = false;
static bool blinkLevel = false;
static bool wifiWasConnected = false;

static void startWifi() {
  Serial.printf("[WIFI ] connecting to \"%s\" ...\n", WIFI_SSID);
  WiFi.disconnect();
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD, WIFI_CHANNEL);
  lastWifiAttemptMs = millis();
}

static void maintainWifi(uint32_t now) {
  const bool connected = (WiFi.status() == WL_CONNECTED);
  if (connected && !wifiWasConnected) {
    Serial.printf("[WIFI ] connected, IP %s, RSSI %d dBm\n",
                  WiFi.localIP().toString().c_str(), WiFi.RSSI());
  } else if (!connected && wifiWasConnected) {
    Serial.println("[WIFI ] connection lost");
  }
  wifiWasConnected = connected;
  if (!connected && now - lastWifiAttemptMs >= RECONNECT_PERIOD_MS) {
    startWifi();
  }
}

static bool publishStatus(const char *payload, bool retained) {
  return mqtt.publish(TOPIC_STATUS, payload, retained);
}

static void maintainMqtt(uint32_t now) {
  if (WiFi.status() != WL_CONNECTED) {
    return;
  }
  if (mqtt.connected()) {
    mqtt.loop();
    return;
  }
  if (mqttAttempted && now - lastMqttAttemptMs < RECONNECT_PERIOD_MS) {
    return;
  }
  Serial.printf("[MQTT ] connecting to %s:%u as %s ...\n", MQTT_HOST, MQTT_PORT, mqttClientId);
  if (mqtt.connect(mqttClientId, nullptr, nullptr, TOPIC_STATUS, 0, true, lwtPayload)) {
    Serial.println("[MQTT ] connected");
    snprintf(statusPayload, sizeof(statusPayload),
             "{\"node\":\"%s\",\"status\":\"online\"}", NODE_ID);
    publishStatus(statusPayload, true);
  } else {
    Serial.printf("[MQTT ] failed, state=%d, retry in %lu s\n", mqtt.state(),
                  static_cast<unsigned long>(RECONNECT_PERIOD_MS / 1000));
  }
  mqttAttempted = true;
  lastMqttAttemptMs = millis();
}

static void startAnomalyPulse(uint32_t now) {
  ledPulseActive = true;
  ledPulseEndMs = now + ANOMALY_PULSE_MS;
}

static void updateLed(uint32_t now) {
  if (ledPulseActive) {
    if (static_cast<int32_t>(now - ledPulseEndMs) >= 0) {
      ledPulseActive = false;
    } else {
      digitalWrite(PIN_LED, HIGH);
      return;
    }
  }
  if (WiFi.status() != WL_CONNECTED || !mqtt.connected()) {
    const uint32_t period = (WiFi.status() != WL_CONNECTED) ? BLINK_NO_WIFI_MS : BLINK_NO_MQTT_MS;
    if (now - lastBlinkMs >= period) {
      lastBlinkMs = now;
      blinkLevel = !blinkLevel;
      digitalWrite(PIN_LED, blinkLevel ? HIGH : LOW);
    }
    return;
  }
  digitalWrite(PIN_LED, LOW);
}

static void processSample(uint32_t now) {
  const uint16_t raw = static_cast<uint16_t>(analogRead(PIN_SENSOR));
  const double volts = raw * ADC_REF_VOLTS / ADC_MAX_COUNTS;
  const float x = static_cast<float>(volts);
  ++sampleSeq;

  const bool voltageSurge = hasPrevVolts && (fabsf(x - prevVolts) >= DV_SURGE_VOLTS);
  const float dv = hasPrevVolts ? (x - prevVolts) : 0.0f;
  prevVolts = x;
  hasPrevVolts = true;

  const FilterDecision d = evaluateSample(window, x, Z_THRESHOLD);
  const bool railFault = (volts <= RAIL_FAULT_LOW_V || volts >= RAIL_FAULT_HIGH_V);

  if (d.flatSignal) {
    ++flatCount;
    if (!inFlatSignal) {
      Serial.printf("[GUARD] seq=%lu sigma=%.6f < %.4f -> flat signal, Z forced to 0\n",
                    static_cast<unsigned long>(sampleSeq), d.stdDev, SIGMA_EPSILON);
    }
  } else if (inFlatSignal && d.state != STATE_WARMUP) {
    Serial.printf("[GUARD] seq=%lu signal varies again (sigma=%.4f)\n",
                  static_cast<unsigned long>(sampleSeq), d.stdDev);
  }
  inFlatSignal = d.flatSignal;

  if (voltageSurge) {
    Serial.printf("[SURGE] seq=%lu dv=%+.3f V (voltage step)\n",
                  static_cast<unsigned long>(sampleSeq), static_cast<double>(dv));
  }
  if (railFault) {
    Serial.printf("[FAULT] seq=%lu v=%.3f V near rail limit (sensor fault)\n",
                  static_cast<unsigned long>(sampleSeq), volts);
  }
  if (d.state == STATE_ANOMALY) {
    startAnomalyPulse(now);
  }

  if (d.state == STATE_NOMINAL) {
    ++droppedCount;
    if (LOG_NOMINAL_SAMPLES) {
      Serial.printf("[DROP ] seq=%lu raw=%u v=%.3f mu=%.3f sd=%.3f z=%+.2f\n",
                    static_cast<unsigned long>(sampleSeq), raw, volts, d.mean, d.stdDev, d.z);
    }
  } else {
    const size_t len = buildTelemetryJson(telemetryPayload, sizeof(telemetryPayload), NODE_ID,
                                          sampleSeq, now, raw, volts, d);
    const bool ok = (len > 0) && mqtt.connected() &&
                    mqtt.publish(TOPIC_TELEMETRY, reinterpret_cast<const uint8_t *>(telemetryPayload),
                                 static_cast<unsigned int>(len), false);
    if (ok) {
      ++sentCount;
      if (d.state == STATE_WARMUP) {
        ++warmupCount;
      } else {
        ++anomalyCount;
      }
    } else {
      ++failedCount;
    }
    Serial.printf("[%s] %s (%u B)%s\n", d.state == STATE_WARMUP ? "WARM " : "ALERT",
                  telemetryPayload, static_cast<unsigned>(len),
                  ok ? "" : (len == 0 ? " NOT SENT (payload overflow)" : " NOT SENT (offline)"));
  }

  const BufferAction action = decideBufferAction(d.state, anomalyRun, GATED_BUFFER_UPDATE);
  if (action == SKIP_ANOMALY) {
    ++skippedCount;
  } else {
    if (action == PUSH_REBASELINE) {
      ++rebaselineCount;
      Serial.printf("[REBAS] seq=%lu run=%lu >= %lu -> v=%.3f pushed into window (level shift, re-baseline)\n",
                    static_cast<unsigned long>(sampleSeq), static_cast<unsigned long>(anomalyRun),
                    static_cast<unsigned long>(REBASELINE_RUN), volts);
    }
    window.push(x);
  }
  if (window.size() == WINDOW_SIZE && sampleSeq == WINDOW_SIZE) {
    Serial.printf("[INFO ] window full (N=%u), filtering active, Z threshold %.2f\n",
                  static_cast<unsigned>(WINDOW_SIZE), Z_THRESHOLD);
  }
}

static void sendHeartbeat(uint32_t now) {
  JsonDocument doc;
  doc["node"] = NODE_ID;
  doc["status"] = "online";
  doc["uptime_s"] = now / 1000;
  doc["sampled"] = sampleSeq;
  doc["sent"] = sentCount;
  doc["warmup"] = warmupCount;
  doc["anomaly"] = anomalyCount;
  doc["dropped"] = droppedCount;
  doc["failed"] = failedCount;
  doc["flat"] = flatCount;
  doc["skipped"] = skippedCount;
  doc["rebaseline"] = rebaselineCount;
  doc["rssi"] = WiFi.RSSI();
  if (measureJson(doc) >= sizeof(statusPayload)) {
    statusPayload[0] = '\0';
    Serial.println("[STAT ] payload overflow, heartbeat skipped");
    return;
  }
  serializeJson(doc, statusPayload, sizeof(statusPayload));
  const bool ok = mqtt.connected() && publishStatus(statusPayload, false);
  Serial.printf("[STAT ] %s%s\n", statusPayload, ok ? "" : " (not sent, offline)");
}

void setup() {
  Serial.begin(115200);
  delay(200);
  Serial.println();
  Serial.println("=== Edge Anomaly Stream Filter - ESP32 edge node ===");
  Serial.printf("N=%u  Z_THRESHOLD=%.2f  fs=%lu Hz  sigma_eps=%.4f\n",
                static_cast<unsigned>(WINDOW_SIZE), static_cast<double>(Z_THRESHOLD),
                static_cast<unsigned long>(1000 / SAMPLE_PERIOD_MS), SIGMA_EPSILON);
  Serial.printf("buffer update: %s, re-baseline after %lu consecutive anomalies\n",
                GATED_BUFFER_UPDATE ? "anomaly-gated" : "legacy (push all)",
                static_cast<unsigned long>(REBASELINE_RUN));

  pinMode(PIN_LED, OUTPUT);
  digitalWrite(PIN_LED, LOW);

  analogReadResolution(12);
#if defined(ADC_ATTEN_DB_12)
  analogSetPinAttenuation(PIN_SENSOR, ADC_ATTEN_DB_12);
#else
  analogSetPinAttenuation(PIN_SENSOR, ADC_11db);
#endif

  const uint64_t mac = ESP.getEfuseMac();
  snprintf(mqttClientId, sizeof(mqttClientId), "%s-%06lX", NODE_ID,
           static_cast<unsigned long>(mac & 0xFFFFFFUL));
  snprintf(lwtPayload, sizeof(lwtPayload),
           "{\"node\":\"%s\",\"status\":\"offline\"}", NODE_ID);

  WiFi.mode(WIFI_STA);
  startWifi();

  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setBufferSize(512);
  mqtt.setKeepAlive(15);
  mqtt.setSocketTimeout(5);

  const uint32_t now = millis();
  lastSampleMs = now;
  lastHeartbeatMs = now;
}

void loop() {
  const uint32_t now = millis();
  maintainWifi(now);
  maintainMqtt(now);

  if (now - lastSampleMs >= SAMPLE_PERIOD_MS) {
    lastSampleMs += SAMPLE_PERIOD_MS;
    if (now - lastSampleMs >= SAMPLE_PERIOD_MS) {
      lastSampleMs = now;
    }
    processSample(now);
  }

  if (now - lastHeartbeatMs >= HEARTBEAT_PERIOD_MS) {
    lastHeartbeatMs = now;
    sendHeartbeat(now);
  }

  updateLed(now);
}
