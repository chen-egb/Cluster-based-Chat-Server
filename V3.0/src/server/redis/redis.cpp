#include "redis.hpp"
#include <iostream>
using namespace std;

Redis::Redis()
    : _publish_context(nullptr), _subscribe_context(nullptr)
{
}

Redis::~Redis()
{
    if (_publish_context != nullptr)
    {
        redisFree(_publish_context);
    }
    if (_subscribe_context != nullptr)
    {
        redisFree(_subscribe_context);
    }
}

// 连接redis服务器
bool Redis::connect()
{
    _publish_context = redisConnect("127.0.0.1", 6379);
    if (_publish_context == nullptr || _publish_context->err)
    {
        cerr << "connect redis failed!" << endl;
        return false;
    }

    _subscribe_context = redisConnect("127.0.0.1", 6379);
    if (_subscribe_context == nullptr || _subscribe_context->err)
    {
        cerr << "connect redis failed!" << endl;
        return false;
    }

    // 在单独线程中监听通道事件
    thread t(&Redis::observer_channel_message, this);
    t.detach();

    cout << "connect redis success!" << endl;
    return true;
}

// 向channel通道发布消息
bool Redis::publish(int channel, string message)
{
    redisReply *reply = (redisReply *)redisCommand(_publish_context, "PUBLISH %d %s", channel, message.c_str());
    if (reply == nullptr)
    {
        cerr << "publish command failed!" << endl;
        return false;
    }
    freeReplyObject(reply);
    return true;
}

// 订阅通道
bool Redis::subscribe(int channel)
{
    if (redisAppendCommand(_subscribe_context, "SUBSCRIBE %d", channel) == REDIS_ERR)
    {
        cerr << "subscribe command failed!" << endl;
        return false;
    }
    redisBufferWrite(_subscribe_context);
    return true;
}

// 取消订阅通道
bool Redis::unsubscribe(int channel)
{
    if (redisAppendCommand(_subscribe_context, "UNSUBSCRIBE %d", channel) == REDIS_ERR)
    {
        cerr << "unsubscribe command failed!" << endl;
        return false;
    }
    redisBufferWrite(_subscribe_context);
    return true;
}

// 在独立线程中接收订阅通道的消息
void Redis::observer_channel_message()
{
    redisReply *reply = nullptr;
    while (REDIS_OK == redisGetReply(_subscribe_context, (void **)&reply))
    {
        if (reply != nullptr && reply->type == REDIS_REPLY_ARRAY && reply->elements == 3)
        {
            if (strcmp(reply->element[0]->str, "message") == 0)
            {
                // 收到消息，给业务层上报
                int channel = atoi(reply->element[1]->str);
                string message = reply->element[2]->str;
                _notify_message_handler(channel, message);
            }
        }
        freeReplyObject(reply);
    }
}

// 初始化业务层上报消息的回调函数
void Redis::init_notify_handler(function<void(int, string)> fn)
{
    this->_notify_message_handler = fn;
}