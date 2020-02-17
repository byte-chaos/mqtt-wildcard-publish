package com.bytechaos.fun.mqtt;

import com.bytechaos.fun.mqtt.subscription.CustomSubscriptionStore;
import com.bytechaos.fun.mqtt.subscription.CustomSubscriptionStoreException;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.packets.publish.PublishPacket;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extension.sdk.api.services.builder.Builders;
import com.hivemq.extension.sdk.api.services.publish.PublishService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @author Abdullah Imal
 */
public class WildcardPublishTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(WildcardPublishTask.class);

    private final @NotNull String topicFilter;
    private final @NotNull PublishPacket publishPacket;
    private final @NotNull CustomSubscriptionStore customSubscriptionStore;
    private final @NotNull PublishService publishService;

    public WildcardPublishTask(final @NotNull String topicFilter, final @NotNull PublishPacket publishPacket) {
        this.topicFilter = topicFilter;
        this.publishPacket = publishPacket;
        customSubscriptionStore = new CustomSubscriptionStore();
        publishService = Services.publishService();
    }

    @Override
    public void run() {
        logger.warn("Received wildcard publish for '{}'. Sending to matching subscriptions. This will take a " +
                        "while...", topicFilter);

        final Set<String> topics;
        try {
            topics = customSubscriptionStore.getTopicsForTopicFilter(topicFilter);
        } catch (final CustomSubscriptionStoreException e) {
            // nooo
            logger.error("Nooo... Could not get all matching topics for topic filter '{}'.", topicFilter, e);
            return;
        }

        for (final String topic : topics) {
            try {
                logger.debug("Sending publish to '{}' because of a wildcard publish.", topic);
                publishService.publish(Builders.publish().fromPublish(publishPacket).topic(topic).build()).get();
            } catch (final InterruptedException | ExecutionException e) {
                logger.error("Oh no... Could not sent to topic '{}‘.", topic, e);
            }
        }

        logger.warn("Succeeded sending a wildcard publish for '{}'.", topicFilter);
        logger.debug("Send wildcard publish for topic filter '{}' to the topics ‘{}‘.", topicFilter, topics);
    }
}
