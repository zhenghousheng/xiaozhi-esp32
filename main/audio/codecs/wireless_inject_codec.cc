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
    bool underrun = to_copy < need;
    if (underrun) {
        // 剩余部分补静音：让 AFE/VAD 持续收到帧，说话结束后能及时判定
        memset(reinterpret_cast<uint8_t*>(dest) + to_copy, 0, need - to_copy);
    }
    xSemaphoreGive(mutex_);

    // under-run 时限速 10ms 防止全速喂帧导致 AFE FEED 环溢出。
    // 必须在释放锁之后等待，否则会阻塞 UDP/TCP 注入任务导致后续帧堆积。
    if (underrun) {
        vTaskDelay(pdMS_TO_TICKS(10));
    }
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
    static_cast<WirelessInjectCodec*>(arg)->TcpTaskLoop();
    vTaskDelete(nullptr);
}

void WirelessInjectCodec::TcpTaskLoop() {
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
        ESP_LOGE(TAG, "WiFi not connected, wireless inject disabled");
        return;
    }
    vTaskDelay(pdMS_TO_TICKS(2000));  // 等 DHCP 拿到 IP

    // 用 TCP 而非 UDP：跨网段 UDP 丢包严重，音频帧丢失会导致 ASR 只听到零星音节。
    // TCP 是字节流，同一连接内顺序可靠送达；帧协议与之前一致。
    while (true) {
        int srv = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
        if (srv < 0) {
            ESP_LOGE(TAG, "Failed to create TCP socket");
            vTaskDelay(pdMS_TO_TICKS(1000));
            continue;
        }
        int reuse = 1;
        setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(kUdpPort);
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        if (bind(srv, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            ESP_LOGE(TAG, "Failed to bind TCP port %u", (unsigned)kUdpPort);
            close(srv);
            vTaskDelay(pdMS_TO_TICKS(1000));
            continue;
        }
        if (listen(srv, 1) < 0) {
            ESP_LOGE(TAG, "Failed to listen");
            close(srv);
            vTaskDelay(pdMS_TO_TICKS(1000));
            continue;
        }
        ESP_LOGI(TAG, "TCP server listening on %u", (unsigned)kUdpPort);

        while (true) {
            sockaddr_in peer{};
            socklen_t peer_len = sizeof(peer);
            int cli = accept(srv, reinterpret_cast<sockaddr*>(&peer), &peer_len);
            if (cli < 0) {
                break;
            }
            ESP_LOGI(TAG, "Client connected");
            // 一次只服务一个连接，客户端断开后回到 accept
            uint8_t buf[2048];
            while (true) {
                ssize_t n = recv(cli, buf, sizeof(buf), 0);
                if (n <= 0) {
                    break;
                }
                for (ssize_t i = 0; i < n; i++) {
                    FeedByte(buf[i]);
                }
            }
            close(cli);
            ESP_LOGI(TAG, "Client disconnected");
        }
        close(srv);
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
                // 幂等触发：仅在待命/提示状态生效，重复到达不会打断进行中的对话
                auto& app = Application::GetInstance();
                auto state = app.GetDeviceState();
                if (state == kDeviceStateIdle || state == kDeviceStateNotifying) {
                    ESP_LOGI(TAG, "Chat trigger from wireless");
                    app.ToggleChatState();
                } else {
                    ESP_LOGW(TAG, "Ignore chat trigger, state=%d", (int)state);
                }
            } else if (cmd_ == 0x03) {
                // 仅在录音中生效：结束录音让服务端立即 ASR。
                // speaking/notifying 时忽略，避免打断正在播放的回答。
                auto& app = Application::GetInstance();
                if (app.GetDeviceState() == kDeviceStateListening) {
                    ESP_LOGI(TAG, "Stop listening from wireless");
                    app.ToggleChatState();
                } else {
                    ESP_LOGW(TAG, "Ignore stop, state=%d", (int)app.GetDeviceState());
                }
            }
            fsm_ = 0;
            break;
        }
        default:
            fsm_ = 0;
            break;
    }
}
