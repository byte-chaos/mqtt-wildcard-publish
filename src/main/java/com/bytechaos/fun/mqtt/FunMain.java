package com.bytechaos.fun.mqtt;

import com.bytechaos.fun.mqtt.interceptor.WildcardPublishCatcher;
import com.hivemq.extension.sdk.api.ExtensionMain;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.parameter.ExtensionStartInput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStartOutput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStopInput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStopOutput;
import com.hivemq.extension.sdk.api.services.Services;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Abdullah Imal
 */
public class FunMain implements ExtensionMain {

    private static final Logger logger = LoggerFactory.getLogger(FunMain.class);

    @Override
    public void extensionStart(final @NotNull ExtensionStartInput extensionStartInput,
                               final @NotNull ExtensionStartOutput extensionStartOutput) {

        logger.warn("MQTT is not designed for Wildcard Publishes! This Extension is built only for fun purposes and " +
                "should not be used for anything else.");

        Services.initializerRegistry().setClientInitializer(
                (initializerInput, clientContext) -> clientContext.addPublishInboundInterceptor(new WildcardPublishCatcher()));
    }

    @Override
    public void extensionStop(final @NotNull ExtensionStopInput extensionStopInput,
                              final @NotNull ExtensionStopOutput extensionStopOutput) {
    }
}
