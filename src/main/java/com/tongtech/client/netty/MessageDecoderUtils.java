package com.tongtech.client.netty;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tongtech.client.broker.ClientChannelInfo;
import com.tongtech.client.enums.CB_REQUEST;
import com.tongtech.client.enums.CN_REQUEST;
import com.tongtech.client.protobuf.ClientMessageData;
import com.tongtech.client.protobuf.RemotingCommand;
import com.tongtech.client.utils.MessageUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 消息解码工具
 * LD备注: 如果后续需要支持其他序列化的方式，可以包装为不同的MessageDecodersUtils，
 * 如ProtoBufMessageDecodersUtils,JSONMessageDecoderUtils
 */
public class MessageDecoderUtils {

    public static Map<String, String> map = new HashMap<>();
    public static BlockingQueue<RemotingCommand> msgQueue = new LinkedBlockingQueue(1000);

    /**
     * 将protobuf解析成RemotingCommand对象[接收客户端请求]
     * @param body
     * @param commandtype
     * @return
     */
    public static RemotingCommand MessageDecoderToRemotingCommand(byte[] body, int commandtype, ChannelHandlerContext ctx) throws InterruptedException {
        RemotingCommand command = new RemotingCommand();
        Object object = null;
        Integer requestId = 0;
        try {
            switch (commandtype) {
                case CB_REQUEST.CB_REQ_CONSUMER_ACK://consumer消费消息确认请求
                    ClientMessageData.CBClientConsumerAck clientConsumerAck = ClientMessageData.CBClientConsumerAck.parseFrom(body);
                    object = clientConsumerAck;
                    requestId = clientConsumerAck.getCommonHeader().getRequestId();
                    System.out.println("收到consumer消费消息确认请求:" + clientConsumerAck);
                    break;
                case CN_REQUEST.CN_REQ_HEARTBEAT://客户端名字服务心跳请求
                case CB_REQUEST.CB_REQ_HEARTBEAT://客户端代理服务心跳请求
                    ClientMessageData.TLQClientHeartbeatRequest beatRequest = ClientMessageData.TLQClientHeartbeatRequest.parseFrom(body);
                    object = beatRequest;
                    requestId = beatRequest.getCommonHeader().getRequestId();
                    //System.out.println("收到客户端心跳请求:" + beatRequest);
                    break;
                case CB_REQUEST.CB_REQ_PULL_MESSAGE://拉取消息请求
                    ClientMessageData.CBClientConsumerPullMsg consumerPull = ClientMessageData.CBClientConsumerPullMsg.parseFrom(body);
                    object = consumerPull;
                    requestId = consumerPull.getCommonHeader().getRequestId();
                    //System.out.println("收到客户端拉取消息请求:" + consumerPull);
//                    System.out.println("消息缓存为:" + msgQueue.toString());
                    break;
                case CN_REQUEST.CN_REQ_REGISTER_CLIENT://客户端名字服务注册请求
                case CN_REQUEST.CN_REQ_UNREGISTER_CLIENT://客户端名字服务注销请求
                case CB_REQUEST.CB_REQ_UNREGISTER_CLIENT://客户端代理服务注销请求
                case CB_REQUEST.CB_REQ_REGISTER_CLIENT://客户端代理注册请求
                    ClientMessageData.Client client = ClientMessageData.Client.parseFrom(body);
                    object = client;
                    requestId = client.getCommonHeader().getRequestId();
                    //System.out.println("收到客户端注册/注销请求:" + client);
                    break;
                case CB_REQUEST.CB_REQ_REGISTER_CONSUMER://consumer注册请求
                    //构建consumer注册报文
                    ClientMessageData.CBRegisterConsumer consumer = ClientMessageData.CBRegisterConsumer.parseFrom(body);
                    object = consumer;
                    requestId = consumer.getCommonHeader().getRequestId();
                    //System.out.println("收到客户端consumer注册请求:" + consumer);
                    break;
                case CB_REQUEST.CB_REQ_REGISTER_PRODUCER://注册producer
                    ClientMessageData.CBRegisterProducer registerProducer = ClientMessageData.CBRegisterProducer.parseFrom(body);
                    object = registerProducer;
                    requestId = registerProducer.getCommonHeader().getRequestId();
//                    System.out.println("收到客户端注册producer请求:" + registerProducer);
                    //保存producer信息到producerManager
                    NettyServer.producerManager.registerProducer(registerProducer.getClientID(),new ClientChannelInfo(ctx.channel(),registerProducer.getClientID(),0));
                    break;
                case CB_REQUEST.CB_SEND_REPLY_MESSAGE:
                case CB_REQUEST.CB_REQ_SEND_MESSAGE://发送消息请求
                    ClientMessageData.MessageBuffer sendMessage = ClientMessageData.MessageBuffer.parseFrom(body);
                    object = sendMessage;
                    requestId = sendMessage.getCommonHeader().getRequestId();
                    System.out.println("收到客户端发送消息请求:" + sendMessage.getMsgHeader().getMsgID() + "--消息内容：" + new String(sendMessage.getData().toByteArray()));
                    if(commandtype == CB_REQUEST.CB_SEND_REPLY_MESSAGE){
                        System.out.println("收到客户端发送响应消息.");
                        sendReplyToClient(sendMessage);
                    }
                    if (map.containsKey(sendMessage.getMsgHeader().getMsgID())) {
                        System.out.println("消息id重复:" + sendMessage.getMsgHeader().getMsgID());
                    } else {
                        map.put(sendMessage.getMsgHeader().getMsgID(), "1");
                    }
                    break;
                case CB_REQUEST.CB_REQ_UNREGISTER_CONSUMER://consumer注销请求
                    ClientMessageData.CBUnRegisterConsumer unRegister = ClientMessageData.CBUnRegisterConsumer.parseFrom(body);
                    object = unRegister;
                    requestId = unRegister.getCommonHeader().getRequestId();
                    //System.out.println("收到客户端consumer注销请求:" + unRegister);
                    break;
                case CB_REQUEST.CB_REQ_UNREGISTER_PRODUCER://注销producer
                    ClientMessageData.CBUnRegisterProducer unRegisterProducer = ClientMessageData.CBUnRegisterProducer.parseFrom(body);
                    object = unRegisterProducer;
                    requestId = unRegisterProducer.getCommonHeader().getRequestId();
//                    System.out.println("收到客户端注销producer请求:" + unRegisterProducer);
                    //注销producer信息到producerManager
                    NettyServer.producerManager.registerProducer(unRegisterProducer.getClientID(),new ClientChannelInfo(ctx.channel(),unRegisterProducer.getClientID(),0));
                    break;
                case CN_REQUEST.CN_REQ_ROUTE://topic路由信息请求
                    ClientMessageData.TLQTopicRouteRequest routeRequest = ClientMessageData.TLQTopicRouteRequest.parseFrom(body);
                    object = routeRequest;
                    requestId = routeRequest.getCommonHeader().getRequestId();
                    //System.out.println("收到客户端topic路由信息请求:" + routeRequest);
                    break;
                case CB_REQUEST.CB_REQ_BATCH_PUSH_MSG://批量发送消息请求
                    ClientMessageData.CBClientBatchPushMsg batchPushMsg = ClientMessageData.CBClientBatchPushMsg.parseFrom(body);
                    object = batchPushMsg;
                    requestId = batchPushMsg.getCommonHeader().getRequestId();
                    System.out.println("收到客户端批量发送消息请求:" + batchPushMsg.getBatchID());
                    //System.out.println("收到客户端批量发送消息请求:" + batchPushMsg);
                    //System.out.println("批量消息id:" + batchPushMsg.getBatchID());
                    break;
                case CN_REQUEST.CN_REQ_CONSUME_ROLLBACK_BY_TIME://与管理节点回溯消息请求
                    ClientMessageData.CNConsumeRollbackByTime rollbackByTime = ClientMessageData.CNConsumeRollbackByTime.parseFrom(body);
                    object = rollbackByTime;
                    requestId = rollbackByTime.getCommonHeader().getRequestId();
                    //System.out.println("收到与管理节点回溯消息请求:" + rollbackByTime);
                    break;

                case CB_REQUEST.CB_REQ_SEND_FILE://传输文件创建请求
                    ClientMessageData.CBSendFileRequest sendFileRequest = ClientMessageData.CBSendFileRequest.parseFrom(body);
                    object = sendFileRequest;
                    requestId = sendFileRequest.getCommonHeader().getRequestId();
                    //System.out.println("收到传输文件创建请求:" + sendFileRequest.getFilePath());
                    //System.out.println("收到传输文件创建请求:" + sendFileRequest.getFilePath());
                    break;

                case CB_REQUEST.CB_REQ_FILE_MESSAGE://传输文件
                    ClientMessageData.MessageFile messageFile = ClientMessageData.MessageFile.parseFrom(body);
                    object = messageFile;
                    requestId = messageFile.getCommonHeader().getRequestId();
                    //System.out.println("收到传输文件beginFileOffset:" + messageFile.getBeginFileOffset()+",endFileOffset:"+messageFile.getEndFileOffset());
                    break;
                case CB_REQUEST.CB_REQ_DOWNLOAD_FILE://下载请求
                    ClientMessageData.CBDownloadRequest downloadRequest = ClientMessageData.CBDownloadRequest.parseFrom(body);
                    object = downloadRequest;
                    requestId = downloadRequest.getCommonHeader().getRequestId();
                    //System.out.println("收到传输文件beginFileOffset:" + messageFile.getBeginFileOffset()+",endFileOffset:"+messageFile.getEndFileOffset());
                    break;
                default:
                    break;
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        //关键参数： Opaque，消息原始对象，指令类型
        command.setOpaque(requestId);
        command.setMessage(object);
        command.setCommandType(commandtype);
        if(commandtype == CB_REQUEST.CB_REQ_SEND_MESSAGE){
            msgQueue.put(command);
//            System.out.println("消息缓存为："+msgQueue.toString());
        }
        return command;
    }

    private static void sendReplyToClient(ClientMessageData.MessageBuffer sendMessage) {
        if (sendMessage.getMsgAttr() != null && !sendMessage.getMsgAttr().getAttrData().isEmpty()) {
            Map<String, Object> properties = (Map<String, Object>) MessageUtils.ByteToObject(sendMessage.getMsgAttr().getAttrData().toByteArray());
            System.out.println("收到客户端发送消息请求Attr:" + properties);
            //立即返回响应
            Object toClient = properties.get("REPLY_TO_CLIENT");
            String toAddr = toClient ==null? null:(String)toClient;
            System.out.println("toAddr:"+toAddr);
            Channel channel = NettyServer.producerManager.getAvaliableChannel(toAddr);
            if(channel==null){
                System.out.println("不存在对应的注册信息。");
                return;
            }
            RemotingCommand cmd = new RemotingCommand();
            cmd.setMessage(sendMessage);
            cmd.setOpaque(sendMessage.getCommonHeader().getRequestId());
            cmd.setCommandType(CB_REQUEST.CB_SEND_REPLY_MESSAGE);
            cmd.setBody(sendMessage.toByteArray());
            cmd.setLength(sendMessage.toByteArray().length);
            //写回消息
            channel.writeAndFlush(cmd);
            //头信息
            //channel.close();
        }
    }
}
