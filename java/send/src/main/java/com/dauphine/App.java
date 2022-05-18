package com.dauphine;

import java.util.concurrent.TimeUnit;

import com.microsoft.azure.servicebus.Message;
import com.microsoft.azure.servicebus.management.ManagementClient;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;

public class App 
{
    static String connectionString = "Endpoint=sb://ironman.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=IUZx7ITn8oiC5VF+7HhcyajmTT4v/U9sumnsBaIF7+s=";
    static String topicName = "dauphine";
    static String sampleMessage = "{\"destination\": {\"station\": 171, \"type\": \"IENA_PTX\", \"scenario\": \"14249582051\", \"screen\": 0 }, \"metadata\": { \"dnsNamesForScenario\": [ \"MGT-A9-078\" ], \"typeScenario\": 7 }, \"train\": { \"mention\": 12, \"departureTime\": \"06/05/2022 15:54:00\", \"sortableTime\": \"06/05/2022 15:54:00\", \"displayInterval\": 90, \"idTransporter\": \"SNCF\", \"trainId\": \"745364\", \"trainType\": \"L\", \"line\": \"E\", \"category\": \"S\", \"destination\": \"Magenta\", \"track\": \"52\", \"platform\": \"2\", \"destinations\": [], \"deleted\": true, \"creationSourceTimestamp\": 1651841564840, \"seuilTempsAttente\": 59, \"prefixeDestination\": \"\", \"suffixeDestination\": \"\", \"notificationMsg\": \"\", \"directions\": [], \"idActivity\": \"Transilien\", \"idCourse\": \"97e02061-84ba-44f7-8f33-8f839eb252bc\", \"idEvent\": \"40\", \"paRestants\": [], \"paDesservisRestants\": [], \"libelleZde\": \"52\", \"removeDuplicateOnTrack\": true } }";
    static String anotherSample = "hello";
    static Sender sender;
    static ManagementClient sbManagementClient = new ManagementClient(
        new ConnectionStringBuilder(connectionString)
    );


    public static void main( String[] args ) throws ServiceBusException, InterruptedException
    {
        // sbManagementClient.createTopic(topicName);
        // TimeUnit.SECONDS.sleep(10);

        sender = new Sender(topicName);

        for(int i = 0; i < 100; i++){
            for(int j = 0; j < 300; j++){
                Sender.Send(new Message(sampleMessage));
            }
            System.out.print("Waiting 5 sec\n");
            TimeUnit.SECONDS.sleep(3);
        }

    }
}

