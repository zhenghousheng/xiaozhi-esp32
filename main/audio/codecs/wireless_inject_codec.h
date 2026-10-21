#ifndef _WIRELESS_INJECT_CODEC_H
#define _WIRELESS_INJECT_CODEC_H

#include "no_audio_codec.h"

#include <freertos/FreeRTOS.h>
#include <freertos/semphr.h>
#include <freertos/task.h>

#include <cstddef>
#include <cstdint>

// 无物理麦克风时，用 PC/手机经 WiFi UDP 灌入 PCM16/16kHz/单声道 模拟麦克风输入。
// 帧协议（PC/手机 -> 设备 IP:UDP 端口）：
//   A5 5A <cmd:1> <len:2 小端> <payload:len>
//   cmd=0x01: PCM 音频数据（16kHz/mono/int16）
//   cmd=0x02: 触发对话（幂等，仅在 idle/notifying 生效，可重复发送）
//   cmd=0x03: 结束录音（仅在 listening 生效，可重复发送）
class WirelessInjectCodec : public NoAudioCodecSimplex {
public:
    WirelessInjectCodec(int input_sample_rate, int output_sample_rate, gpio_num_t spk_bclk,
                        gpio_num_t spk_ws, gpio_num_t spk_dout, gpio_num_t mic_sck,
                        gpio_num_t mic_ws, gpio_num_t mic_din);
    ~WirelessInjectCodec();

    void Start() override;

    // UDP 收包任务调用：把灌入的帧 payload 写入环形缓冲
    void InjectAudio(const uint8_t* data, size_t len);

private:
    static constexpr uint16_t kUdpPort = 8765;         // 设备监听端口
    static constexpr size_t kRingSize = 16000 * 2 * 3;  // 3 秒缓冲
    static constexpr uint16_t kMaxPayload = 2048;

    int Read(int16_t* dest, int samples) override;
    void EnableInput(bool enable) override;

    static void UdpTaskEntry(void* arg);
    void TcpTaskLoop();
    void FeedByte(uint8_t byte);
    // 帧收完整后立即执行。必须在收帧处同步调用，不能靠"下一个字节"
    // 触发一个额外的 fsm 状态，否则那一个字节会被白白吞掉。
    void ExecuteFrame();

    uint8_t* ring_ = nullptr;
    volatile size_t head_ = 0;
    volatile size_t tail_ = 0;
    SemaphoreHandle_t mutex_ = nullptr;
    TaskHandle_t udp_task_ = nullptr;

    // 帧解析状态机
    int fsm_ = 0;
    uint8_t cmd_ = 0;
    uint16_t len_ = 0;
    uint16_t idx_ = 0;
    uint8_t payload_[kMaxPayload];

    // 诊断计数（每个会话打印一次）
    uint32_t cmd1_count_ = 0;
    uint32_t cmd2_count_ = 0;
    uint32_t cmd3_count_ = 0;
    uint32_t bad_frames_ = 0;
};

#endif  // _WIRELESS_INJECT_CODEC_H
