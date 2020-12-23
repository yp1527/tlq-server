package com.tongtech.client.netty;

import com.google.protobuf.ByteString;
import com.tongtech.client.domain.FileMsg;
import com.tongtech.client.enums.*;
import com.tongtech.client.protobuf.ClientMessageData;
import com.tongtech.client.protobuf.CommonHeader;
import com.tongtech.client.protobuf.RemotingCommand;
import com.tongtech.client.utils.FileUtils;
import com.tongtech.client.utils.IpUtils;
import io.netty.util.ReferenceCountUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 编码工具类
 */
public class MessageEncoderUtils {
    private static FileChannel fileChannel;
    private static int sumFile=0;
    private static AtomicInteger requestId = new AtomicInteger(1);
    private static AtomicInteger offset = new AtomicInteger(1);
    private static AtomicInteger messageCount = new AtomicInteger(1);

    private static ConcurrentMap<String,String>managerCache=new ConcurrentHashMap<>();
    private static ConcurrentMap<String,String>workCache=new ConcurrentHashMap<>();
    //<msgId,<fileId,FileMsg>>
    private static ConcurrentMap<String, ConcurrentHashMap<String,FileMsg>>fileMap=new ConcurrentHashMap<>();

    public static final String fileSendPath = "D:/netty/send";
    public static final String fileReceivePath = "D:/netty/receive/service";
    public static final int verNo=0;
    public static int sum=0;
    public static int count=0;

    public static int total=0;

    public static boolean flag=true;

    public static int messageSize=110;

    public static String producerId="";

    public static long fileSize=1024*1024*4;

    public static List<ClientMessageData.MessageBuffer>messageBufferList=new ArrayList<>();
    static {
        for(int i=0;i<11000;i++){
            ClientMessageData.MessageBuffer messageBuffer=getMessageBuffer("topic1");
            messageBufferList.add(messageBuffer);
        }
        /*ConcurrentHashMap<String,FileMsg> pp=new ConcurrentHashMap<>();
        try {
            File createFile = new File(fileReceivePath, "tmp_1kk.mp4");
            FileChannel channel=(FileChannel.open(createFile.toPath(),
                    StandardOpenOption.WRITE, StandardOpenOption.APPEND));
            FileMsg msg=getFileMsg(981561467,299916318,"7faa8bf787ab28dd1027b62fb3cb6251","3a0ae0ef8390f43879c174a760f1051a",
                    245390366,490780732,1,"tmp_1kk.mp4",channel);
            pp.put("3a0ae0ef8390f43879c174a760f1051a",msg);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            File createFile = new File(fileReceivePath, "tmp_0kk.mp4");
            FileChannel channel=(FileChannel.open(createFile.toPath(),
                    StandardOpenOption.WRITE, StandardOpenOption.APPEND));
            FileMsg msg=getFileMsg(981561467,58720256,"7faa8bf787ab28dd1027b62fb3cb6251","1f0e4984761d5c45ec50ae82dcbf643c",
                    0,245390366,0,"tmp_0kk.mp4",channel);
            pp.put("1f0e4984761d5c45ec50ae82dcbf643c",msg);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            File createFile = new File(fileReceivePath, "tmp_2kk.mp4");
            FileChannel channel=(FileChannel.open(createFile.toPath(),
                    StandardOpenOption.WRITE, StandardOpenOption.APPEND));
            FileMsg msg=getFileMsg(981561467,549500988,"7faa8bf787ab28dd1027b62fb3cb6251","0dc01d918c280bb59cdbcf9a2781f76e",
                    490780732,736171098,2,"tmp_2kk.mp4",channel);
            pp.put("0dc01d918c280bb59cdbcf9a2781f76e",msg);
        } catch (IOException e) {
            e.printStackTrace();
        }

        fileMap.put("7faa8bf787ab28dd1027b62fb3cb6251",pp);*/

        /*ConcurrentHashMap<String,FileMsg> pp=new ConcurrentHashMap<>();
        try {
            File createFile = new File(fileReceivePath, "tmp_0kk.mp4");
            FileChannel channel=(FileChannel.open(createFile.toPath(),
                    StandardOpenOption.WRITE, StandardOpenOption.APPEND));
            FileMsg msg=getFileMsg(981561467,486539264,"7faa8bf787ab28dd1027b62fb3cb6251","458961694723543040",
                    0,981561467,0,"tmp_0kk.mp4",channel);
            pp.put("458961694723543040",msg);
        } catch (IOException e) {
            e.printStackTrace();
        }
        fileMap.put("7faa8bf787ab28dd1027b62fb3cb6251",pp);*/
    }

    public static FileMsg getFileMsg(long fileSiz,long fileOffset,String fileHash,String splitFileHash,long startPosition,long endPosition,int index,String fileName,FileChannel channel){
        FileMsg msg=new FileMsg();
        msg.setIndex(index);
        msg.setSplitFileHash(splitFileHash);
        msg.setFileHash(fileHash);
        msg.setStartPosition(startPosition);
        msg.setEndPosition(endPosition);
        msg.setFileOffset(fileOffset);
        msg.setSourceSize(fileSiz);
        msg.setRealFileSize(fileSiz);
        msg.setFileName(fileName);
        msg.setFileChannel(channel);
        return msg;
    }


    public static RemotingCommand MessageEncoderToRemotingCommand(CommonMessage message,RemotingCommand remotingRequest) throws Exception {
        RemotingCommand remotingCommand=new RemotingCommand();
        //头信息
        CommonHeader.Common.Builder common = CommonHeader.Common.newBuilder();
        common.setVerNo(verNo);
        common.setRequestId(message.getRequestId());
        int length=0;
        byte[]body=null;
        switch (message.getCommandType()) {
            case CN_REQUEST.CN_REQ_HEARTBEAT://管理节点心跳响应
                common.setCommandType(CN_RESPONSE.CN_RSP_HEARTBEAT);
                remotingCommand.setCommandType(CN_RESPONSE.CN_RSP_HEARTBEAT);
                ClientMessageData.TLQClientHeartbeatRequest heartbeatRequest=(ClientMessageData.TLQClientHeartbeatRequest)remotingRequest.getMessage();

                ClientMessageData.TLQClientHeartbeatResponse.Builder clientHeartbeatResponse = ClientMessageData.TLQClientHeartbeatResponse.newBuilder();
                ClientMessageData.TopicBrokerInfo.Builder topicBrokerInfo = ClientMessageData.TopicBrokerInfo.newBuilder();
                topicBrokerInfo.setIpaddr(IpUtils.IpToInt("127.0.0.1"));
                //topicBrokerInfo.setIpaddr6("0:0:0:0:0:0:0:1");
                topicBrokerInfo.setPort(8080);
                topicBrokerInfo.setTopicName("topic");
                topicBrokerInfo.setState(1);
                if(producerId!=null&&!"".equals(producerId)){
                    topicBrokerInfo.setProducerId(producerId);
                }
                if (count%5==0){
                    //clientHeartbeatResponse.addMsgInfo(topicBrokerInfo);
                }
                count++;
                clientHeartbeatResponse.setCommonHeader(common);
                ClientMessageData.TLQClientHeartbeatResponse heartbeatResponse=clientHeartbeatResponse.build();
                length=heartbeatResponse.toByteArray().length;
                body=heartbeatResponse.toByteArray();
                break;

            case CB_REQUEST.CB_REQ_HEARTBEAT://工作节点心跳响应
                ClientMessageData.TLQClientHeartbeatRequest clientHeartbeatRequest=(ClientMessageData.TLQClientHeartbeatRequest)remotingRequest.getMessage();
                if(workCache.get(clientHeartbeatRequest.getClientId())!=null){
                    common.setCommandType(CB_RESPONSE.CB_RSP_HEARTBEAT);
                    remotingCommand.setCommandType(CB_RESPONSE.CB_RSP_HEARTBEAT);
                    ClientMessageData.TLQClientBrokerHeartbeatResponse.Builder brokerHeart = ClientMessageData.TLQClientBrokerHeartbeatResponse.newBuilder();
                    brokerHeart.setCommonHeader(common);
                    brokerHeart.setClientId(clientHeartbeatRequest.getClientId());
                    ClientMessageData.TLQClientBrokerHeartbeatResponse build=brokerHeart.build();
                    length=build.toByteArray().length;
                    body=build.toByteArray();
                }
                break;
            case CB_REQUEST.CB_REQ_PULL_MESSAGE://客户端拉取消息响应
                ClientMessageData.CBClientConsumerPullMsg pullMsg=(ClientMessageData.CBClientConsumerPullMsg)remotingRequest.getMessage();
                int status=1;
                if(status==1){
                    //文件响应
                    common.setCommandType(CB_RESPONSE.CB_RSP_CONSUME_FILE);
                    remotingCommand.setCommandType(CB_RESPONSE.CB_RSP_CONSUME_FILE);
                    File file=new File("D:/netty/receive/service/kk.mp4");
                    ClientMessageData.CBConsumerFileResponse.Builder cbBrokerPushMsg = ClientMessageData.CBConsumerFileResponse.newBuilder();
                    cbBrokerPushMsg.setCommonHeader(common);
                    byte []data=null;
                    if(file.length()>fileSize){
                        //大于4M;
                    }else {
                        data=getFileData(file,0,file.length());
                        cbBrokerPushMsg.setData(ByteString.copyFrom(data));
                    }
                    String hash= FileUtils.getFileMD5(file);
                    cbBrokerPushMsg.setMsgID(hash);
                    cbBrokerPushMsg.setHash(hash);
                    cbBrokerPushMsg.setRealFileSize(file.length());
                    cbBrokerPushMsg.setSourceSize(file.length());
                    cbBrokerPushMsg.setFilename(file.getName());
                    cbBrokerPushMsg.setStatusCode(0);
                    cbBrokerPushMsg.setFileID(9999999999999l);

                    ClientMessageData.CBConsumerFileResponse pushMsg=cbBrokerPushMsg.build();
                    length=pushMsg.toByteArray().length;
                    body=pushMsg.toByteArray();
                }else {
                    common.setCommandType(CB_RESPONSE.CB_RSP_PULL_MESSAGE);
                    remotingCommand.setCommandType(CB_RESPONSE.CB_RSP_PULL_MESSAGE);
                    ClientMessageData.CBBrokerPushMsg.Builder cbBrokerPushMsg = ClientMessageData.CBBrokerPushMsg.newBuilder();
                    cbBrokerPushMsg.setCommonHeader(common);
                    cbBrokerPushMsg.setQueueID(0);
                    cbBrokerPushMsg.setClientID(pullMsg.getClientID());
                    cbBrokerPushMsg.setConsumerID(pullMsg.getConsumerID());
                    cbBrokerPushMsg.setGroupName(pullMsg.getGroupName());
                    cbBrokerPushMsg.setTopic(pullMsg.getTopic());

                    cbBrokerPushMsg.setStatusCode(0);
                    if(pullMsg.getConsumeQueOffset()==-1){
                        cbBrokerPushMsg.setMinConsumeQueueOffset(95);
                        cbBrokerPushMsg.setMaxConsumeQueueOffset(100);
                        //拉取最新消息
                        for(int m=95;m<=100;m++){
                            ClientMessageData.MessageBuffer messageBuffer=messageBufferList.get(m-1);
                            cbBrokerPushMsg.addMessages(messageBuffer);
                        }
                    }else if(pullMsg.getConsumeQueOffset()>=0){
                        long end=0;
                        if(total>=10){
                            messageSize=11000;
                            if(flag){
                                for(int i=0;i<10;i++){
                                    ClientMessageData.MessageBuffer messageBuffer=getMessageBuffer("topic1");
                                    messageBufferList.add(messageBuffer);
                                }
                            }
                            flag=false;
                            total=0;
                        }
                        if(pullMsg.getConsumeQueOffset()+pullMsg.getPullNum()>=messageSize){
                            end=messageSize;
                            total++;
                        }else {
                            end=pullMsg.getConsumeQueOffset()+pullMsg.getPullNum();
                        }
                        cbBrokerPushMsg.setMinConsumeQueueOffset(pullMsg.getConsumeQueOffset());
                        cbBrokerPushMsg.setMaxConsumeQueueOffset(end);
                        for(long m=pullMsg.getConsumeQueOffset();m<end;m++){
                            ClientMessageData.MessageBuffer messageBuffer=messageBufferList.get((int)(m));
                            cbBrokerPushMsg.addMessages(messageBuffer);
                        }
                    }
                    sum++;
                    ClientMessageData.CBBrokerPushMsg pushMsg=cbBrokerPushMsg.build();
                    length=pushMsg.toByteArray().length;
                    body=pushMsg.toByteArray();
                }
                break;
            case CN_REQUEST.CN_REQ_UNREGISTER_CLIENT://管理节点注销响应[2]
            case CN_REQUEST.CN_REQ_REGISTER_CLIENT://管理节点注册响应[1]
            case CB_REQUEST.CB_REQ_UNREGISTER_CLIENT://工作节点注销响应[1002]
            case CB_REQUEST.CB_REQ_REGISTER_CLIENT://工作节点注册响应[1001]
                ClientMessageData.Client clientRequest = (ClientMessageData.Client)remotingRequest.getMessage();
                if(message.getCommandType()==CN_REQUEST.CN_REQ_UNREGISTER_CLIENT){
                    managerCache.remove(clientRequest.getClientId());
                    common.setCommandType(CN_RESPONSE.CN_RSP_UNREGISTER_CLIENT);
                    remotingCommand.setCommandType(CN_RESPONSE.CN_RSP_UNREGISTER_CLIENT);
                }else if(message.getCommandType()==CN_REQUEST.CN_REQ_REGISTER_CLIENT){
                    //把客户端id存在managerCache中
                    managerCache.put(clientRequest.getClientId(),"1");
                    common.setCommandType(CN_RESPONSE.CN_RSP_REGISTER_CLIENT);
                    remotingCommand.setCommandType(CN_RESPONSE.CN_RSP_REGISTER_CLIENT);
                }else if(message.getCommandType()==CB_REQUEST.CB_REQ_REGISTER_CLIENT){
                    //把客户端id保存在workCache中
                    workCache.put(clientRequest.getClientId(),"1");
                    common.setCommandType(CB_RESPONSE.CB_RSP_REGISTER_CLIENT);
                    remotingCommand.setCommandType(CB_RESPONSE.CB_RSP_REGISTER_CLIENT);
                }else if(message.getCommandType()==CB_REQUEST.CB_REQ_UNREGISTER_CLIENT){
                    workCache.remove(clientRequest.getClientId());
                    common.setCommandType(CB_RESPONSE.CB_RSP_UNREGISTER_CLIENT);
                    remotingCommand.setCommandType(CB_RESPONSE.CB_RSP_UNREGISTER_CLIENT);
                }
                ClientMessageData.Client.Builder cbClient = ClientMessageData.Client.newBuilder();
                cbClient.setCommonHeader(common);
                //String clientId = UUID.randomUUID().toString();
                cbClient.setClientId(clientRequest.getClientId());
                cbClient.setStatusCode(0);
                cbClient.setIdentifier(clientRequest.getIdentifier());
                ClientMessageData.Client client=cbClient.build();
                length=client.toByteArray().length;
                body=client.toByteArray();
                break;
            case CB_REQUEST.CB_REQ_REGISTER_CONSUMER://注册consumer响应[1005]
                ClientMessageData.CBRegisterConsumer registerConsumer = (ClientMessageData.CBRegisterConsumer)remotingRequest.getMessage();
                ClientMessageData.CBRegisterConsumerAck.Builder registerConsumerAck = ClientMessageData.CBRegisterConsumerAck.newBuilder();
                common.setCommandType(CB_RESPONSE.CB_RSP_REGISTER_CONSUMER);
                remotingCommand.setCommandType(CB_RESPONSE.CB_RSP_REGISTER_CONSUMER);
                registerConsumerAck.setCommonHeader(common);
                registerConsumerAck.setClientID(registerConsumer.getClientID());
                registerConsumerAck.setGroupName(registerConsumer.getGroupName());
                registerConsumerAck.setTopic(registerConsumer.getTopic());
                registerConsumerAck.setConsumerID(registerConsumer.getConsumerID());
                registerConsumerAck.setStatusCode(0);
                ClientMessageData.CBRegisterConsumerAck consumerAck=registerConsumerAck.build();
                length=consumerAck.toByteArray().length;
                body=consumerAck.toByteArray();
                break;
            case CB_REQUEST.CB_REQ_REGISTER_PRODUCER:
                common.setCommandType(CB_RESPONSE.CB_RSP_REGISTER_PRODUCER);
                remotingCommand.setCommandType(CB_RESPONSE.CB_RSP_REGISTER_PRODUCER);
                ClientMessageData.CBRegisterProducer registerProducer = (ClientMessageData.CBRegisterProducer)remotingRequest.getMessage();

                ClientMessageData.CBRegisterProducerAck.Builder registerProducerAck = ClientMessageData.CBRegisterProducerAck.newBuilder();
                registerProducerAck.setCommonHeader(common);
                registerProducerAck.setClientID(registerProducer.getClientID());
                registerProducerAck.setProducerID(registerProducer.getProducerID());
                registerProducerAck.setIdentifier(registerProducer.getIdentifier());
                registerProducerAck.setStatusCode(0);
                ClientMessageData.CBRegisterProducerAck registerProducerAck1=registerProducerAck.build();
                length=registerProducerAck1.toByteArray().length;
                body=registerProducerAck1.toByteArray();

                break;
            case CB_REQUEST.CB_REQ_SEND_MESSAGE://发送消息响应
                /*try {
                    Thread.sleep(4000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/
                ClientMessageData.MessageBuffer sendMessage = (ClientMessageData.MessageBuffer)remotingRequest.getMessage();
                ClientMessageData.MessageHeader header=sendMessage.getMsgHeader();
                ClientMessageData.PublicMsgHeader publicMsgHeader=header.getPubHeader();
                common.setCommandType(CB_RESPONSE.CB_RSP_SEND_MESSAGE_ACK);
                remotingCommand.setCommandType(CB_RESPONSE.CB_RSP_SEND_MESSAGE_ACK);
                ClientMessageData.MessageAck.Builder messageAck = ClientMessageData.MessageAck.newBuilder();
                messageAck.setClientID(publicMsgHeader.getClientID());
                messageAck.setMsgID(header.getMsgID());
                messageAck.setProducerID(sendMessage.getProducerID());
                messageAck.setStatusCode(0);
                messageAck.setCommonHeader(common);
                ClientMessageData.MessageAck ack=messageAck.build();
                length=ack.toByteArray().length;
                body=ack.toByteArray();
                messageCount.getAndIncrement();
                //System.out.println("消息总数:"+messageCount.get());
                break;
            case CB_REQUEST.CB_REQ_UNREGISTER_CONSUMER://注销consumer响应
                common.setCommandType(CB_RESPONSE.CB_RSP_UNREGISTER_CONSUMER);
                remotingCommand.setCommandType(CB_RESPONSE.CB_RSP_UNREGISTER_CONSUMER);
                ClientMessageData.CBUnRegisterConsumer unRegisterConsumer = (ClientMessageData.CBUnRegisterConsumer)remotingRequest.getMessage();
                ClientMessageData.CBUnRegisterConsumerAck.Builder unRegisterConsumerAck = ClientMessageData.CBUnRegisterConsumerAck.newBuilder();
                unRegisterConsumerAck.setCommonHeader(common);
                unRegisterConsumerAck.setClientID(unRegisterConsumer.getClientID());
                unRegisterConsumerAck.setConsumerID(unRegisterConsumer.getConsumerID());
                unRegisterConsumerAck.setIdentifier(unRegisterConsumer.getIdentifier());
                unRegisterConsumerAck.setStatusCode(0);
                ClientMessageData.CBUnRegisterConsumerAck unRegisterConsumerAck1=unRegisterConsumerAck.build();
                length=unRegisterConsumerAck1.toByteArray().length;
                body=unRegisterConsumerAck1.toByteArray();
                break;
            case CB_REQUEST.CB_REQ_UNREGISTER_PRODUCER:
                common.setCommandType(CB_RESPONSE.CB_RSP_UNREGISTER_PRODUCER);
                remotingCommand.setCommandType(CB_RESPONSE.CB_RSP_UNREGISTER_PRODUCER);
                ClientMessageData.CBUnRegisterProducer unRegisterProducer = (ClientMessageData.CBUnRegisterProducer)remotingRequest.getMessage();

                ClientMessageData.CBUnRegisterProducerAck.Builder unRegisterProducerAck = ClientMessageData.CBUnRegisterProducerAck.newBuilder();
                unRegisterProducerAck.setCommonHeader(common);
                unRegisterProducerAck.setClientID(unRegisterProducer.getClientID());
                unRegisterProducerAck.setProducerID(unRegisterProducer.getProducerID());
                unRegisterProducerAck.setIdentifier(unRegisterProducer.getIdentifier());
                unRegisterProducerAck.setStatusCode(0);
                ClientMessageData.CBUnRegisterProducerAck unRegisterProducerAck1=unRegisterProducerAck.build();
                length=unRegisterProducerAck1.toByteArray().length;
                body=unRegisterProducerAck1.toByteArray();
                break;
            case CN_REQUEST.CN_REQ_ROUTE://Topic路由信息响应
                ClientMessageData.TLQTopicRouteRequest routeRequest=(ClientMessageData.TLQTopicRouteRequest)remotingRequest.getMessage();
                common.setCommandType(CN_RESPONSE.CN_RSP_ROUTE);
                remotingCommand.setCommandType(CN_RESPONSE.CN_RSP_ROUTE);
                ClientMessageData.TLQTopicRouteResponse.Builder topicRouteResponse = ClientMessageData.TLQTopicRouteResponse.newBuilder();

                ClientMessageData.TopicBrokerInfo.Builder topicInfo = ClientMessageData.TopicBrokerInfo.newBuilder();
                topicInfo.setIpaddr(IpUtils.IpToInt("127.0.0.1"));
                //topicInfo.setIpaddr(IpUtils.IpToInt("192.168.56.1"));
                //topicInfo.setIpaddr6("0:0:0:0:0:0:0:1");
                topicInfo.setPort(9999);
                topicInfo.setTopicName(routeRequest.getTopicName());
                topicInfo.setState(1);
                topicInfo.setProducerId(routeRequest.getProducerId());
                producerId=routeRequest.getProducerId();
                topicRouteResponse.addMsgInfo(topicInfo);

                /*ClientMessageData.TopicBrokerInfo.Builder topicInfo1 = ClientMessageData.TopicBrokerInfo.newBuilder();
                topicInfo1.setIpaddr(IpUtils.IpToInt("127.0.0.1"));
                //topicInfo1.setIpaddr6("0:0:0:0:0:0:0:1");
                topicInfo1.setPort(9090);
                topicInfo1.setTopicName(routeRequest.getTopicName());
                topicInfo1.setState(1);
                topicInfo1.setProducerId(routeRequest.getProducerId());
                topicRouteResponse.addMsgInfo(topicInfo1);*/

                /*if(count<=10){
                    ClientMessageData.TopicBrokerInfo.Builder topicInfo = ClientMessageData.TopicBrokerInfo.newBuilder();
                    topicInfo.setIpaddr(IpUtils.IpToInt("47.104.138.62"));
                    //topicInfo.setIpaddr6("0:0:0:0:0:0:0:1");
                    topicInfo.setPort(8080);
                    topicInfo.setTopicName(routeRequest.getTopicName());
                    topicInfo.setState(1);
                    topicInfo.setProducerId(routeRequest.getProducerId());
                    producerId=routeRequest.getProducerId();
                    topicRouteResponse.addMsgInfo(topicInfo);

                    ClientMessageData.TopicBrokerInfo.Builder topicInfo1 = ClientMessageData.TopicBrokerInfo.newBuilder();
                    topicInfo1.setIpaddr(IpUtils.IpToInt("127.0.0.1"));
                    //topicInfo1.setIpaddr6("0:0:0:0:0:0:0:1");
                    topicInfo1.setPort(9090);
                    topicInfo1.setTopicName(routeRequest.getTopicName());
                    topicInfo1.setState(1);
                    topicInfo1.setProducerId(routeRequest.getProducerId());
                    topicRouteResponse.addMsgInfo(topicInfo1);
                }else {
                    ClientMessageData.TopicBrokerInfo.Builder topicInfo = ClientMessageData.TopicBrokerInfo.newBuilder();
                    topicInfo.setIpaddr(IpUtils.IpToInt("127.0.0.1"));
                    //topicInfo.setIpaddr6("0:0:0:0:0:0:0:1");
                    topicInfo.setPort(8080);
                    topicInfo.setTopicName(routeRequest.getTopicName());
                    topicInfo.setState(1);
                    topicInfo.setProducerId(routeRequest.getProducerId());
                    producerId=routeRequest.getProducerId();
                    topicRouteResponse.addMsgInfo(topicInfo);
                }*/
                topicRouteResponse.setCommonHeader(common);
                ClientMessageData.TLQTopicRouteResponse routeResponse=topicRouteResponse.build();
                length=routeResponse.toByteArray().length;
                body=routeResponse.toByteArray();
                break;
            case CB_REQUEST.CB_REQ_BATCH_PUSH_MSG://批量发送消息请求
                /*try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/
                //int l=5/0;
                ClientMessageData.CBClientBatchPushMsg batchPushMsgRequest =(ClientMessageData.CBClientBatchPushMsg)remotingRequest.getMessage();
                common.setCommandType(CB_RESPONSE.CB_RSP_BATCH_PUSH_MSG_ACK);
                remotingCommand.setCommandType(CB_RESPONSE.CB_RSP_BATCH_PUSH_MSG_ACK);
                ClientMessageData.CBClientBatchPushMsgAck.Builder pushAck = ClientMessageData.CBClientBatchPushMsgAck.newBuilder();
                pushAck.setCommonHeader(common);
                pushAck.setBatchID(batchPushMsgRequest.getBatchID());
                pushAck.setClientID(batchPushMsgRequest.getClientID());
                pushAck.setStatusCode(0);
                ClientMessageData.CBClientBatchPushMsgAck pushAckResponse=pushAck.build();
                length=pushAckResponse.toByteArray().length;
                body=pushAckResponse.toByteArray();
                break;
            case CN_REQUEST.CN_REQ_CONSUME_ROLLBACK_BY_TIME://与管理节点回溯消息请求
                ClientMessageData.CNConsumeRollbackByTime rollbackByTime =(ClientMessageData.CNConsumeRollbackByTime)remotingRequest.getMessage();
                common.setCommandType(CN_RESPONSE.CN_RSP_CONSUME_ROLLBACK_BY_TIME_ACK);
                remotingCommand.setCommandType(CN_RESPONSE.CN_RSP_CONSUME_ROLLBACK_BY_TIME_ACK);

                ClientMessageData.CNConsumeRollbackByTimeAck.Builder rollBackAck = ClientMessageData.CNConsumeRollbackByTimeAck.newBuilder();
                rollBackAck.setCommonHeader(common);
                rollBackAck.setClientID(rollbackByTime.getClientID());
                rollBackAck.setResultCode(0);

                ClientMessageData.TopicBrokerInfo.Builder info = ClientMessageData.TopicBrokerInfo.newBuilder();
                info.setIpaddr(IpUtils.IpToInt("47.104.138.62"));
                //info.setIpaddr6("0:0:0:0:0:0:0:1");
                info.setPort(9999);
                info.setTopicName(rollbackByTime.getTopicName());
                info.setState(1);
                rollBackAck.addMsgInfo(info);

                ClientMessageData.CNConsumeRollbackByTimeAck rollbackResponse=rollBackAck.build();
                length=rollbackResponse.toByteArray().length;
                body=rollbackResponse.toByteArray();
                break;
            case CB_REQUEST.CB_REQ_SEND_FILE://文件传输创建请求
                ClientMessageData.SendFileRequest sendFileRequest =(ClientMessageData.SendFileRequest)remotingRequest.getMessage();
                common.setCommandType(CB_RESPONSE.CB_RSP_SEND_FILE);
                remotingCommand.setCommandType(CB_RESPONSE.CB_RSP_SEND_FILE);
                ClientMessageData.SendFileResponse.Builder response = ClientMessageData.SendFileResponse.newBuilder();
                ConcurrentMap<String,FileMsg>map=null;
                FileMsg fileMsg1=null;
                //<msgId,<fileId,FileMsg>>
                if(fileMap.containsKey(sendFileRequest.getFileHash())){
                    map=fileMap.get(sendFileRequest.getFileHash());
                    if(map.containsKey(sendFileRequest.getSplitFileHash())){
                        response.setType(1);
                        fileMsg1=map.get(sendFileRequest.getSplitFileHash());
                    }else {
                        String fileName="tmp_"+sendFileRequest.getIndex()+sendFileRequest.getFileName();
                        File createFile = new File(fileReceivePath, fileName);
                        createFile.createNewFile();
                        fileMsg1=new FileMsg();
                        fileMsg1.setFileHash(sendFileRequest.getFileHash());
                        fileMsg1.setFileName(fileName);
                        fileMsg1.setRealFileSize(sendFileRequest.getRealFileSize());
                        fileMsg1.setSourceSize(sendFileRequest.getSourceSize());
                        fileMsg1.setBreakFlag(sendFileRequest.getBreakFlag());
                        fileMsg1.setFilePath(sendFileRequest.getFilePath());
                        fileMsg1.setStartPosition(sendFileRequest.getStartPosition());
                        fileMsg1.setEndPosition(sendFileRequest.getEndPosition());
                        fileMsg1.setSplitFileHash(sendFileRequest.getSplitFileHash());
                        fileMsg1.setIndex(sendFileRequest.getIndex());
                        FileChannel channel=(FileChannel.open(createFile.toPath(),
                                StandardOpenOption.WRITE, StandardOpenOption.APPEND));
                        if(sendFileRequest.getBreakFlag()==0){
                            ByteBuffer wrap = ByteBuffer.wrap(sendFileRequest.getData().toByteArray());
                            try {
                                int size=channel.write(wrap);
                                fileMsg1.setFileOffset(fileMsg1.getFileOffset()+size);
                                channel.force(true);
                                ReferenceCountUtil.release(wrap);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        fileMsg1.setFileChannel(channel);
                        map.put(sendFileRequest.getSplitFileHash(),fileMsg1);
                        fileMap.put(sendFileRequest.getFileHash(), (ConcurrentHashMap<String, FileMsg>) map);
                        response.setType(0);
                    }
                }else {//文件不存在
                    map=new ConcurrentHashMap<String, FileMsg>();
                    String fileName="tmp_"+sendFileRequest.getIndex()+sendFileRequest.getFileName();
                    File createFile = new File(fileReceivePath, fileName);
                    createFile.createNewFile();
                    fileMsg1=new FileMsg();
                    fileMsg1.setFileHash(sendFileRequest.getFileHash());
                    fileMsg1.setFileName(fileName);
                    fileMsg1.setRealFileSize(sendFileRequest.getRealFileSize());
                    fileMsg1.setSourceSize(sendFileRequest.getSourceSize());
                    fileMsg1.setBreakFlag(sendFileRequest.getBreakFlag());
                    fileMsg1.setFilePath(sendFileRequest.getFilePath());
                    fileMsg1.setStartPosition(sendFileRequest.getStartPosition());
                    fileMsg1.setEndPosition(sendFileRequest.getEndPosition());
                    fileMsg1.setSplitFileHash(sendFileRequest.getSplitFileHash());
                    fileMsg1.setIndex(sendFileRequest.getIndex());
                    FileChannel fileChannel=(FileChannel.open(createFile.toPath(),
                            StandardOpenOption.WRITE, StandardOpenOption.APPEND));

                    if(sendFileRequest.getBreakFlag()==0){
                        ByteBuffer wrap = ByteBuffer.wrap(sendFileRequest.getData().toByteArray());
                        try {
                            int size=fileChannel.write(wrap);
                            fileMsg1.setFileOffset(fileMsg1.getFileOffset()+size);
                            fileChannel.force(true);
                            ReferenceCountUtil.release(wrap);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    fileMsg1.setFileChannel(fileChannel);
                    map.put(sendFileRequest.getSplitFileHash(),fileMsg1);
                    fileMap.put(sendFileRequest.getFileHash(), (ConcurrentHashMap<String, FileMsg>) map);
                    response.setType(0);
                }
                //486539264
                System.out.println("getFileOffset:"+fileMsg1.getFileOffset());
                response.setCommonHeader(common);
                response.setFileOffset(fileMsg1.getFileOffset());
                response.setMsgID(sendFileRequest.getFileHash());
                response.setFileID(Long.parseLong(sendFileRequest.getSplitFileHash()));
                response.setStatusCode(0);

                ClientMessageData.SendFileResponse fileResponse=response.build();
                length=fileResponse.toByteArray().length;
                body=fileResponse.toByteArray();
                break;
            case CB_REQUEST.CB_REQ_FILE_MESSAGE://文件传输请求
                ClientMessageData.MessageFile messageFile =(ClientMessageData.MessageFile)remotingRequest.getMessage();
                common.setCommandType(CB_RESPONSE.CB_RSP_FILE_MESSAGE_ACK);
                remotingCommand.setCommandType(CB_RESPONSE.CB_RSP_FILE_MESSAGE_ACK);
                ClientMessageData.MessageFileAck.Builder fileAck = ClientMessageData.MessageFileAck.newBuilder();
                if(fileMap.containsKey(messageFile.getMsgID())){
                    ConcurrentMap<String,FileMsg> data=fileMap.get(messageFile.getMsgID());
                    FileMsg fileMsg=data.get(String.valueOf(messageFile.getFileID()));

                    System.out.println("当前传输文件：BeginFileOffset:"+messageFile.getBeginFileOffset()+"--->EndFileOffset:"+messageFile.getEndFileOffset());
                    if (messageFile.getData() != null) {
                        /*if(messageFile.getBeginFileOffset()==486539264){
                            System.out.println("进入异常....");
                            System.out.println("已经保存的位置偏移量:"+fileMsg.getFileOffset());
                            System.out.println("fileMsg:"+fileMsg.toString());
                            throw new Exception("自定义异常....");
                        }*/
                        //setStartPosition:486539264--->setEndPosition490733568
                        FileChannel channel=fileMsg.getFileChannel();
                        ByteBuffer wrap = ByteBuffer.wrap(messageFile.getData().toByteArray());
                        try {
                            channel.write(wrap);
                            channel.force(true);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        fileMsg.setFileOffset(fileMsg.getFileOffset() + messageFile.getEndFileOffset()-messageFile.getBeginFileOffset());
                        ReferenceCountUtil.release(wrap);
                        if(channel.size()>=(fileMsg.getEndPosition()-fileMsg.getStartPosition())){
                            //传输完成
                            fileAck.setType(1);
                        }else {
                            fileAck.setType(0);
                        }

                    }
                    System.out.println("当前传输fileMsg："+fileMsg.toString());
                    fileAck.setCommonHeader(common);
                    fileAck.setFileOffset(fileMsg.getFileOffset());
                    fileAck.setMsgID(messageFile.getMsgID());
                    fileAck.setFileID(messageFile.getFileID());
                    fileAck.setStatusCode(0);

                    ClientMessageData.MessageFileAck messageFileAck=fileAck.build();
                    length=messageFileAck.toByteArray().length;
                    body=messageFileAck.toByteArray();
                }
                break;

            case CB_REQUEST.CB_REQ_DOWNLOAD_FILE://下载请求
                ClientMessageData.CBDownloadRequest downloadRequest =(ClientMessageData.CBDownloadRequest)remotingRequest.getMessage();
                common.setCommandType(CB_RESPONSE.CB_RSP_DOWNLOAD_FILE);
                remotingCommand.setCommandType(CB_RESPONSE.CB_RSP_DOWNLOAD_FILE);

                ClientMessageData.CBDownloadResponse.Builder donwnload = ClientMessageData.CBDownloadResponse.newBuilder();
                donwnload.setCommonHeader(common);
                File file=new File("D:/netty/receive/service/kk.mp4");
                donwnload.setMsgID(downloadRequest.getMsgID());
                donwnload.setConsumerID(downloadRequest.getConsumerID());
                donwnload.setBeginFileOffset(downloadRequest.getBeginOffset());
                donwnload.setEndFileOffset(downloadRequest.getEndOffset());
                donwnload.setFileID(downloadRequest.getFileID());
                byte []data=getFileData(file,downloadRequest.getBeginOffset(),downloadRequest.getEndOffset()-downloadRequest.getBeginOffset());
                donwnload.setData(ByteString.copyFrom(data));
                donwnload.setStatusCode(0);
                ClientMessageData.CBDownloadResponse downloadResponse=donwnload.build();
                length=downloadResponse.toByteArray().length;
                body=downloadResponse.toByteArray();
                break;
            default:
                break;
        }
        remotingCommand.setLength(length);
        remotingCommand.setBody(body);
        remotingCommand.setOpaque(message.getRequestId());
        remotingCommand.setVerNo(message.getVerNo());
        return remotingCommand;
    }

    public static byte[] getFileData(File file,long position,long size){
        byte[] arr=null;
        FileChannel fileChannel=null;
        try {
            fileChannel = (FileChannel.open(file.toPath(),StandardOpenOption.READ));
            MappedByteBuffer map = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, size).load();
            map.asReadOnlyBuffer().flip();
            arr = new byte[map.asReadOnlyBuffer().remaining()];
            map.asReadOnlyBuffer().get(arr);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(fileChannel!=null){
                try {
                    fileChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return arr;
    }

    public static ClientMessageData.MessageBuffer getMessageBuffer(String topic){
        ClientMessageData.MessageBuffer.Builder buffer=ClientMessageData.MessageBuffer.newBuilder();
        buffer.setData(ByteString.copyFrom(("hello world-"+sum).getBytes()));
        Map<String,Object>map=new HashMap<>();
        map.put("key","yangping");
        ClientMessageData.MessageAttr.Builder attr=ClientMessageData.MessageAttr.newBuilder();
        attr.setAttrCount(1);
        attr.setAttrData(ByteString.copyFrom(mapToBytes(map)));
        buffer.setMsgAttr(attr);
        buffer.setProducerID("producerId");

        ClientMessageData.MessageHeader.Builder header=ClientMessageData.MessageHeader.newBuilder();
        header.setConsumeQueueOffset(offset.getAndIncrement());
        header.setTopicName(topic);
        header.setMsgID(UUID.randomUUID().toString());
        header.setQueueID(0);
        header.setExpiry(200);
        header.setPersistence(1);
        header.setCluster("cluster");
        header.setDomain("");
        header.setCommitLogOffset(requestId.getAndIncrement());
        header.setBrokerId(100);
        int time=(int)System.currentTimeMillis()/1000;
        header.setTime(time);
        buffer.setMsgHeader(header);
        return buffer.build();
    }

    public static byte[] mapToBytes(Map<String, Object> map) {
        byte[] bytes = null;
        ByteArrayOutputStream byteArrayOutputStream=null;
        ObjectOutputStream objectOutputStream=null;
        try {
            byteArrayOutputStream = new ByteArrayOutputStream();
            objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(map);
            bytes = byteArrayOutputStream.toByteArray();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(byteArrayOutputStream!=null){
                try {
                    byteArrayOutputStream.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            if(objectOutputStream!=null){
                try {
                    objectOutputStream.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        return bytes;
    }
}
