package com.dauphine;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;

import com.microsoft.azure.servicebus.Message;
import com.microsoft.azure.servicebus.TopicClient;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;

public class Sender {
    
    //String connectionString = System.getenv("CONNECTION_STRING");
    static String connectionString = "";
    static TopicClient topicClient;
    static ArrayList<Message> messages = new ArrayList<Message>();
    static int batchMaxSize = 500;
    static int batchMaxDelay = 10000;
    static Date date = new Date();
    static long lastBatchTimestamp = 0;
    

    public Sender(String topicName) throws InterruptedException, ServiceBusException{
        topicClient = new TopicClient(
            new ConnectionStringBuilder(connectionString, topicName)
        );
    }

    public static void Send(Message message){

        if(messages.size() == 0) {
            lastBatchTimestamp = date.getTime();
            System.out.print("Setting batch timestamp : " + lastBatchTimestamp + '\n');
        }

        messages.add(message);

        // If timestamp is expired or messagebatch size is over expected max size
        if(messages.size() >= batchMaxSize | (date.getTime() > lastBatchTimestamp + batchMaxDelay)){
            sendBatchAndClearList();
        }
    }

    private static void sendBatchAndClearList(){
        System.out.print("Sending Batch\n");
        topicClient.sendBatchAsync(messages);
        messages.clear();
    }
}
