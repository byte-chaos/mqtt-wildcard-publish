package com.bytechaos.fun.mqtt.interceptor;

import com.bytechaos.fun.mqtt.WildcardPublishTask;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.interceptor.publish.PublishInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundInput;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundOutput;
import com.hivemq.extension.sdk.api.services.ManagedExtensionExecutorService;
import com.hivemq.extension.sdk.api.services.Services;

/**
 * @author Abdullah Imal
 */
public class WildcardPublishCatcher implements PublishInboundInterceptor {

    private final @NotNull ManagedExtensionExecutorService executorService;

    public WildcardPublishCatcher() {
        executorService = Services.extensionExecutorService();
    }

    @Override
    public void onInboundPublish(final @NotNull PublishInboundInput publishInboundInput,
                                 final @NotNull PublishInboundOutput publishInboundOutput) {

        final String topic = publishInboundInput.getPublishPacket().getTopic();

        if (topic.contains("HASH") || topic.contains("PLUS")) {
            executorService.submit(new WildcardPublishTask(topic, publishInboundInput.getPublishPacket()));
            publishInboundOutput.preventPublishDelivery();
        }
    }
}
