package com.bytechaos.fun.mqtt.subscription;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extension.sdk.api.services.subscription.SubscriptionStore;
import com.hivemq.extension.sdk.api.services.subscription.TopicSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * @author Abdullah Imal
 */
public class CustomSubscriptionStore {

    private static final Logger logger = LoggerFactory.getLogger(CustomSubscriptionStore.class);

    private static final String FUN = "FUN";

    private final @NotNull SubscriptionStore subscriptionStore;

    public CustomSubscriptionStore() {
        subscriptionStore = Services.subscriptionStore();
    }

    public @NotNull Set<String> getTopicsForTopicFilter(final @NotNull String topicFilter) throws CustomSubscriptionStoreException {

        final Set<String> topics;

        if (topicFilter.contentEquals("HASH") || topicFilter.contentEquals("PLUS")) {
            topics = getAllTopics();
        } else {
            topics = new HashSet<>();
            try {
                subscriptionStore.iterateAllSubscriptions((context, value) -> {
                    for (final TopicSubscription subscription : value.getSubscriptions()) {
                        if (matches(topicFilter, subscription.getTopicFilter())) {
                            topics.add(subscription.getTopicFilter());
                        }
                    }
                }).get();
            } catch (final InterruptedException | ExecutionException e) {
                throw new CustomSubscriptionStoreException("Could not get all subscriptions.", e);
            }
        }

        return replaceWildcardSymbols(topics);
    }

    private static boolean matches(final @NotNull String topicFilter, final @NotNull String subscribedTopicFilter) {

        final String[] splitTopicFilter = topicFilter.split("/");
        final String[] splitSubTopicFilter = subscribedTopicFilter.split("/");

        for (int i = 0; i < splitTopicFilter.length && i < splitSubTopicFilter.length; i++) {

            final boolean lastElement = i + 1 == splitTopicFilter.length;
            final boolean bothLastElement = lastElement && i + 1 == splitSubTopicFilter.length;

            if (splitTopicFilter[i].contentEquals("HASH") || splitSubTopicFilter[i].contentEquals("#")) {
                return true;
            }

            if (splitTopicFilter[i].contentEquals("PLUS")) {
                if (bothLastElement) {
                    return true;
                }
                continue;
            }
            if (splitTopicFilter[i].contentEquals(splitSubTopicFilter[i])) {
                if (lastElement) {
                    return true;
                }
                continue;
            }
        }
        return false;
    }

    private @NotNull Set<String> getAllTopics() throws CustomSubscriptionStoreException {

        final Set<String> topics = new HashSet<>();

        try {
            subscriptionStore.iterateAllSubscriptions((context, value) -> {
                topics.addAll(
                        value.getSubscriptions()
                                .stream()
                                .map(TopicSubscription::getTopicFilter)
                                .collect(Collectors.toSet())
                );
            }).get();
        } catch (final InterruptedException | ExecutionException e) {
            throw new CustomSubscriptionStoreException("Could not get all subscriptions.", e);
        }

        return topics;
    }


    private static @NotNull Set<String> replaceWildcardSymbols(final @NotNull Set<String> uniqueTopics) {
        final List<String> modifiableList = new ArrayList<>(uniqueTopics);

        for (int i = 0; i < modifiableList.size(); i++) {
            final String topic = modifiableList.get(i);
            logger.debug("Wildcard Publish. Processing topic '{}'.", topic);
            if (topic.contains("#")) {
                modifiableList.set(i, modifiableList.get(i).replace("#", FUN)); // Add a little fun.
            }
            if (topic.contains("+")) {
                modifiableList.set(i, modifiableList.get(i).replace("+", FUN)); // And a bit here.
            }
            logger.debug("Wildcard Publish. Topic after processing '{}'.", modifiableList.get(i));
        }

        return Set.copyOf(modifiableList);
    }
}
