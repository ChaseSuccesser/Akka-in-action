package com.ligx.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Created by Administrator on 2016/7/16.
 */
public class Client {

    public static void main(String[] args) {
        TTransport transport;
        try {
            transport = new TFramedTransport(new TSocket("127.0.0.1", 8080));

            TProtocol protocol = new TBinaryProtocol(transport);

            HelloWorldService.Client client = new HelloWorldService.Client(protocol);
            transport.open();
            String result = client.hello("ligx");
            System.out.println(result);
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
