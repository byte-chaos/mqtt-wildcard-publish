package com.bytechaos.fun.mqtt.subscription;

import com.hivemq.extension.sdk.api.annotations.NotNull;

/**
 * @author Abdullah Imal
 */
public class CustomSubscriptionStoreException extends Exception {

    public CustomSubscriptionStoreException(final @NotNull String message, final @NotNull Throwable cause) {
        super(message, cause);
    }
}
