package com.tongtech.client.enums;

/**
 * 客户端-代理请求报文 1001~1500
 */
public interface CB_REQUEST {
   public static final int CB_REQ_REGISTER_CLIENT = 1001,
    CB_REQ_UNREGISTER_CLIENT = 1002,
    CB_REQ_REGISTER_PRODUCER = 1003,
    CB_REQ_UNREGISTER_PRODUCER = 1004,
    CB_REQ_REGISTER_CONSUMER = 1005,
    CB_REQ_UNREGISTER_CONSUMER = 1006,
    CB_REQ_SEND_MESSAGE = 1007,
    CB_REQ_PULL_MESSAGE = 1008,
    CB_REQ_HEARTBEAT = 1009,
    CB_REQ_CONSUMER_ACK = 1010,
   CB_REQ_BATCH_PUSH_MSG=1011;
}
