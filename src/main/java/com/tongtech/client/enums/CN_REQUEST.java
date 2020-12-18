package com.tongtech.client.enums;

/**
 * 客户端-名字服务请求报文 1~500
 */
public interface CN_REQUEST {
    public static final int CN_REQ_REGISTER_CLIENT = 1,
    CN_REQ_UNREGISTER_CLIENT = 2,
    CN_REQ_HEARTBEAT = 3,
    CN_REQ_ROUTE = 4,
    //回溯消息请求类型
    CN_REQ_CONSUME_ROLLBACK_BY_TIME = 5;
}
