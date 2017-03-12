package com.ligx.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;

/**
 * Created by Administrator on 2016/7/16.
 */
public class Client {

    public static void main(String[] args) throws IOException {
        try {
            TNonblockingSocket transport = new TNonblockingSocket("127.0.0.1", 8080);

            TProtocolFactory protocol = new TBinaryProtocol.Factory();

            TAsyncClientManager clientManager = new TAsyncClientManager();
            MethodCallback callback = new MethodCallback();
            HelloWorldService.AsyncClient asyncClient = new HelloWorldService.AsyncClient(protocol, clientManager, transport);

            asyncClient.hello("ligx", callback);
            Object resp = callback.getResult();
            while(resp == null){
                resp = callback.getResult();
            }
            System.out.println(((HelloWorldService.AsyncClient.hello_call)resp).getResult());


//            HelloWorldService.Client client = new HelloWorldService.Client(protocol);
//            transport.open();
//            String result = client.hello("ligx");
//            System.out.println(result);
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
