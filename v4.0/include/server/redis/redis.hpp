#ifndef REDIS_H
#define REDIS_H

#include <hiredis/hiredis.h>
#include <thread>
#include <functional>
using namespace std;

class Redis
{
public:
    Redis();
    ~Redis();

    // 连接Redis服务器
    bool connect();

    // 向Redis指定通道发布消息
    bool publish(int channel, string message);

    // 向Redis订阅通道消息
    bool subscribe(int channel);

    // 取消订阅
    bool unsubscribe(int channel);

    // 在独立线程中接收订阅通道中的消息
    void observer_channel_message();

    // 初始化向业务层上报消息的回调函数
    void init_notify_handler(function<void(int, string)> fn);

private:
    // hiredis同步上下文对象，负责publish消息
    redisContext *_publish_context;

    // hiredis同步上下文对象，负责subscribe消息
    redisContext *_subscribe_context;

    // 回调操作，收到订阅的消息，给service层上报
    function<void(int, string)> _notify_message_handler;
};

#endif