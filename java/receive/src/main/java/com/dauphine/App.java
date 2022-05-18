package com.dauphine;

import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;

import java.time.Duration;
import java.util.concurrent.*;

public class App 
{
    public static void main( String[] args ) throws InterruptedException, ServiceBusException
    {
        ExecutorService executorService = Executors.newWorkStealingPool();

        SubscriptionClient subscribtionClient = new SubscriptionClient(
            new ConnectionStringBuilder(
                "",
                "dauphine-publi-0/subscriptions/dauphine"),
            ReceiveMode.RECEIVEANDDELETE 
        );

        subscribtionClient.registerMessageHandler(
            new IMessageHandler() {

                @Override
                public CompletableFuture<Void> onMessageAsync(IMessage message) {
                    if(message.getMessageBody() != null){
                        System.out.println(message.getMessageBody().toString());
                    }
                    return CompletableFuture.completedFuture(null);
                }

                @Override
                public void notifyException(Throwable exception, ExceptionPhase phase) {
                    System.out.println(exception.getMessage());
                }
                
            },
            new MessageHandlerOptions(10, true, Duration.ofMinutes(1)),
            executorService
        );

    }
}
