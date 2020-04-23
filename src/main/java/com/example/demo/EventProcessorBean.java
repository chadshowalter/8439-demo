package com.example.demo;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.EventContext;
import com.azure.messaging.eventhubs.models.PartitionContext;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
@Slf4j
public class EventProcessorBean {


    public EventProcessorBean(
            @Value("${azure.eventhub.connection-string:}") String eventHubConnectionString,
            @Value("${azure.eventhub.name:}") String eventHubName,
            @Value("${azure.blobstorage.connection-string:}") String storageConnectionString,
            @Value("${azure.blobstorage.container-name:}") String storageContainerName) {
        BlobContainerAsyncClient blobClient = new BlobContainerClientBuilder()
                .connectionString(storageConnectionString)
                .containerName(storageContainerName)
                .buildAsyncClient();

        EventProcessorClient processor = new EventProcessorClientBuilder()
                .connectionString(eventHubConnectionString, eventHubName)
                .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
                .checkpointStore(new BlobCheckpointStore(blobClient))
                .processEvent(this::onEvent)
                .processError(context -> {
                    log.error("Error occurred on partition: {}.",
                            context.getPartitionContext().getPartitionId(), context.getThrowable());
                })
                .processPartitionInitialization(initializationContext -> {
                    log.info("Started receiving on partition: {}",
                            initializationContext.getPartitionContext().getPartitionId());
                })
                .processPartitionClose(closeContext -> {
                    log.info("Stopped receiving on partition: {}. Reason: {}",
                            closeContext.getPartitionContext().getPartitionId(),
                            closeContext.getCloseReason());

                })
                .buildEventProcessorClient();

        processor.start();
        log.info("Processor started");
    }

    private void onEvent(EventContext eventContext) {
        EventData event = eventContext.getEventData();
        PartitionContext partition = eventContext.getPartitionContext();
        log.info("Processing event from partition {} with sequence number {}",
                partition.getPartitionId(), event.getSequenceNumber());

        log.info("Contents: {}",  new String(event.getBody(), StandardCharsets.UTF_8));

        //update checkpoint every 10 events
        if (eventContext.getEventData().getSequenceNumber() % 10 == 0) {
            log.info("Updating checkpoint at sequence number {}", eventContext.getEventData().getSequenceNumber());
            eventContext.updateCheckpoint();
        }
    }



}
