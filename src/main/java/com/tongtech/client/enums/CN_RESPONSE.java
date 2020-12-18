package com.tongtech.client.enums;

/**
 * 客户端-名字服务应答报文 501~1000
 */
public interface CN_RESPONSE {
    public static final int CN_RSP_REGISTER_CLIENT = 501,
    CN_RSP_UNREGISTER_CLIENT = 502,
    CN_RSP_HEARTBEAT = 503,
    CN_RSP_ROUTE = 504,
    CN_RSP_CONSUME_ROLLBACK_BY_TIME_ACK = 505;
}
