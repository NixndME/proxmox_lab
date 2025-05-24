package com.morpheusdata.proxmox.ve.util

import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j
import java.io.IOException
import java.net.SocketTimeoutException
import java.net.ConnectException

/**
 * Utility for executing Proxmox API actions with retry and exponential backoff.
 */
@Slf4j
class ProxmoxRetryUtil {

    /**
     * Executes the given closure with retry logic using exponential backoff.
     * Transient failures are retried up to {@code maxAttempts} times.
     *
     * @param action the code block returning a ServiceResponse
     * @param actionDesc text description used for logging
     * @param maxAttempts number of attempts, defaults to 3
     * @param baseDelay initial delay in milliseconds, defaults to 1000
     * @return ServiceResponse from the last attempt
     */
    static ServiceResponse executeWithRetry(Closure<ServiceResponse> action,
                                            String actionDesc,
                                            int maxAttempts = 3,
                                            long baseDelay = 1000L) {
        int attempt = 0
        long delay = baseDelay
        ServiceResponse resp
        while(attempt < maxAttempts) {
            attempt++
            try {
                resp = action.call()
                if(resp?.success || !ProxmoxApiUtil.isTransientFailure(resp)) {
                    return resp
                }
                log.warn("${actionDesc} attempt ${attempt} of ${maxAttempts} failed: ${resp?.msg ?: resp?.content}")
            } catch(IOException | SocketTimeoutException | ConnectException e) {
                resp = ServiceResponse.error(e.message)
                log.warn("${actionDesc} attempt ${attempt} of ${maxAttempts} threw exception: ${e.message}")
            } catch(Exception e) {
                log.error("${actionDesc} failed with error: ${e.message}", e)
                return ServiceResponse.error(e.message)
            }
            if(attempt < maxAttempts) {
                try {
                    Thread.sleep(delay)
                } catch(InterruptedException ignore) {
                    Thread.currentThread().interrupt()
                }
                delay *= 2
            }
        }
        log.error("${actionDesc} failed after ${maxAttempts} attempts: ${resp?.msg ?: 'Unknown error'}")
        return resp ?: ServiceResponse.error("${actionDesc} failed")
    }
}
