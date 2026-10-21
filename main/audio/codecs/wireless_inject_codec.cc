#include "wireless_inject_codec.h"
#include "application.h"

#include <esp_log.h>
#include <esp_heap_caps.h>
#include <esp_wifi.h>
#include <lwip/sockets.h>
#include <string.h>

#include <sys/select.h>

#define TAG "WirelessInjectCodec"

WirelessInjectCodec::WirelessInjectCodec(int input_sample_rate, int output_sample_rate,
                                        gpio_num_t spk_bclk, gpio_num_t spk_ws, gpio_num_t spk_dout,
                                        gpio_num_t mic_sck, gpio_num_t mic_ws, gpio_num_t mic_din)
    : NoAudioCodecSimplex(input_sample_rate, output_sample_rate, spk_bclk, spk_ws, spk_dout,
                          mic_sck, mic_ws, mic_din) {
    // 环形缓冲放 PSRAM（设备有 8MB PSRAM）
    ring_ = static_cast<uint8_t*>(heap_caps_malloc(kRingSize, MALLOC_CAP_SPIRAM));
    if (ring_ == nullptr) {
        ring_ = static_cast<uint8_t*>(malloc(kRingSize));
    }
    if (ring_ == nullptr) {
        ESP_LOGE(TAG, "Failed to allocate ring buffer");
        return;
    }
    mutex_ = xSemaphoreCreateMutex();
    ESP_LOGI(TAG, "Wireless inject codec created (udp port %u, ring %u bytes)",
             (unsigned)kUdpPort, (unsigned)kRingSize);
}

WirelessInjectCodec::~WirelessInjectCodec() {
    if (udp_task_ != nullptr) {
        vTaskDelete(udp_task_);
    }
    if (mutex_ != nullptr) {
        vSemaphoreDelete(mutex_);
    }
    if (ring_ != nullptr) {
        free(ring_);
    }
}

void WirelessInjectCodec::Start() {
    NoAudioCodecSimplex::Start();

    // UDP 任务：收 PC/手机灌入的音频帧。喇叭 I2S 输出不受影响。
    if (udp_task_ == nullptr && ring_ != nullptr) {
        xTaskCreatePinnedToCore(UdpTaskEntry, "wireless_inject", 4096, this, 4, &udp_task_, 1);
        ESP_LOGI(TAG, "Wireless inject task started, listening UDP %u", (unsigned)kUdpPort);
    }
}

void WirelessInjectCodec::InjectAudio(const uint8_t* data, size_t len) {
    if (ring_ == nullptr || len == 0) {
        return;
    }
    xSemaphoreTake(mutex_, portMAX_DELAY);
    size_t avail = (head_ + kRingSize - tail_) % kRingSize;
    if (avail + len > kRingSize) {
        // 溢出丢最旧数据（PC/手机侧应限速在 32KB/s 以内）
        size_t drop = avail + len - kRingSize;
        tail_ = (tail_ + drop) % kRingSize;
        ESP_LOGW(TAG, "Ring overflow, dropped %u bytes", (unsigned)drop);
    }
    for (size_t i = 0; i < len; i++) {
        ring_[head_] = data[i];
        head_ = (head_ + 1) % kRingSize;
    }
    xSemaphoreGive(mutex_);
}

int WirelessInjectCodec::Read(int16_t* dest, int samples) {
    size_t need = static_cast<size_t>(samples) * sizeof(int16_t);
    if (ring_ == nullptr) {
        memset(dest, 0, need);
        return samples;  // 无缓冲时输出静音，保持 VAD 正常工作
    }
    xSemaphoreTake(mutex_, portMAX_DELAY);
    size_t avail = (head_ + kRingSize - tail_) % kRingSize;
    size_t to_copy = (avail < need) ? avail : need;
    for (size_t i = 0; i < to_copy; i++) {
        reinterpret_cast<uint8_t*>(dest)[i] = ring_[tail_];
        tail_ = (tail_ + 1) % kRingSize;
    }
    // 剩余部分补静音：让 AFE/VAD 持续收到帧，说话结束后能及时判定
    if (to_copy < need) {
        memset(reinterpret_cast<uint8_t*>(dest) + to_copy, 0, need - to_copy);
    }
    xSemaphoreGive(mutex_);
    return samples;
}

void WirelessInjectCodec::EnableInput(bool enable) {
    if (enable == input_enabled_) {
        return;
    }
    // 不操作 I2S RX 通道（无物理麦克风），只翻转标志
    input_enabled_ = enable;
    ESP_LOGI(TAG, "Wireless input enable -> %s", enable ? "true" : "false");
}

void WirelessInjectCodec::UdpTaskEntry(void* arg) {
    static_cast<WirelessInjectCodec*>(arg)->UdpTaskLoop();
    vTaskDelete(nullptr);
}

void WirelessInjectCodec::UdpTaskLoop() {
    // 必须等 WiFi 连接后再建 socket：本任务启动时 lwIP/esp_netif 可能尚未
    // 初始化（AudioService 早于 WifiBoard），直接 socket() 会触发
    // tcpip_send_msg_wait_sem assert 导致启动循环。
    bool connected = false;
    for (int i = 0; i < 600 && !connected; i++) {  // 最多等 60 秒
        wifi_ap_record_t ap_info{};
        if (esp_wifi_sta_get_ap_info(&ap_info) == ESP_OK) {
            connected = true;
            break;
        }
        vTaskDelay(pdMS_TO_TICKS(100));
    }
    if (!connected) {
        ESP_LOGE(TAG, "WiFi not connected, UDP inject disabled");
        return;
    }
    vTaskDelay(pdMS_TO_TICKS(2000));  // 等 DHCP 拿到 IP
    ESP_LOGI(TAG, "WiFi connected, creating UDP socket");

    int sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (sock < 0) {
        ESP_LOGE(TAG, "Failed to create UDP socket");
        return;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(kUdpPort);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ESP_LOGE(TAG, "Failed to bind UDP port %u", (unsigned)kUdpPort);
        close(sock);
        return;
    }
    ESP_LOGI(TAG, "UDP socket bound on %u", (unsigned)kUdpPort);

    uint8_t buf[2048];
    while (true) {
        ssize_t n = recvfrom(sock, buf, sizeof(buf), 0, nullptr, nullptr);
        if (n <= 0) {
            vTaskDelay(pdMS_TO_TICKS(10));
            continue;
        }
        for (ssize_t i = 0; i < n; i++) {
            FeedByte(buf[i]);
        }
    }
}

void WirelessInjectCodec::FeedByte(uint8_t byte) {
    switch (fsm_) {
        case 0:  // 等帧头第一字节
            fsm_ = (byte == 0xA5) ? 1 : 0;
            break;
        case 1:  // 等帧头第二字节
            if (byte == 0x5A) {
                fsm_ = 2;
            } else {
                fsm_ = (byte == 0xA5) ? 1 : 0;
            }
            break;
        case 2:  // cmd
            cmd_ = byte;
            fsm_ = 3;
            break;
        case 3:  // len 低字节
            len_ = byte;
            fsm_ = 4;
            break;
        case 4:  // len 高字节
            len_ |= static_cast<uint16_t>(byte) << 8;
            idx_ = 0;
            fsm_ = (len_ == 0) ? 6 : 5;
            break;
        case 5:  // payload（超长部分计数后丢弃）
            if (idx_ < kMaxPayload) {
                payload_[idx_++] = byte;
            } else if (idx_ < 0xFFFF) {
                idx_++;
            }
            if (idx_ >= len_) {
                fsm_ = 6;
            }
            break;
        case 6: {  // 帧结束，执行
            uint16_t payload_len = (len_ > kMaxPayload) ? kMaxPayload : len_;
            if (cmd_ == 0x01 && payload_len > 0) {
                InjectAudio(payload_, payload_len);
            } else if (cmd_ == 0x02) {
                ESP_LOGI(TAG, "Chat trigger from wireless");
                Application::GetInstance().ToggleChatState();
            }
            fsm_ = 0;
            break;
        }
        default:
            fsm_ = 0;
            break;
    }
}
