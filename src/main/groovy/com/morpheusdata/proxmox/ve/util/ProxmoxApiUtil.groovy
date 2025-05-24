package com.morpheusdata.proxmox.ve.util

import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j

@Slf4j
class ProxmoxApiUtil {
    /**
     * Executes an API request with retry logic for transient errors.
     * @param client the HttpApiClient
     * @param apiUrl the base URL
     * @param path   API path
     * @param queryParams query params
     * @param body request body
     * @param opts request options
     * @param method HTTP method
     * @param maxAttempts number of attempts
     * @param baseDelay initial backoff in ms
     * @return ServiceResponse
     */
    static ServiceResponse callJsonApiWithRetry(HttpApiClient client,
                                                String apiUrl,
                                                String path,
                                                Map queryParams = null,
                                                Map body = null,
                                                HttpApiClient.RequestOptions opts = null,
                                                String method = 'GET',
                                                int maxAttempts = 3,
                                                long baseDelay = 1000L) {
        int attempt = 0
        ServiceResponse resp
        long delay = baseDelay
        while(attempt < maxAttempts) {
            attempt++
            try {
                resp = client.callJsonApi(apiUrl, path, queryParams, body, opts, method)
                if(resp?.success || !isTransientFailure(resp) || attempt >= maxAttempts) {
                    return resp
                }
                log.warn("Transient API failure on ${path} - attempt ${attempt} of ${maxAttempts}: ${resp?.msg ?: resp?.content}")
            } catch(Exception e) {
                if(attempt >= maxAttempts) {
                    log.error("API call to ${path} failed after ${attempt} attempts: ${e.message}", e)
                    return ServiceResponse.error("API call to ${path} failed: ${e.message}")
                }
                log.warn("API call to ${path} attempt ${attempt} failed: ${e.message}")
                resp = ServiceResponse.error(e.message)
            }
            sleep(delay)
            delay *= 2
        }
        return resp ?: ServiceResponse.error("Unknown error calling ${path}")
    }

    static boolean isTransientFailure(ServiceResponse resp) {
        Integer code = resp?.errorCode ?: resp?.statusCode
        if(code == null) return true
        return (code >= 500 && code < 600) || code in [408,429]
    }

    static ServiceResponse validateApiResponse(ServiceResponse resp, String actionDescription) {
        if(resp?.success) {
            return resp
        }
        String detail = resp?.msg ?: resp?.content ?: 'No additional error detail provided by API.'
        if(detail.length() > 300) detail = detail.take(300) + '...'
        String errorMsg = "${actionDescription}. ErrorCode: ${resp?.errorCode ?: 'N/A'}. HTTP Status: ${resp?.statusCode ?: 'N/A'}. Detail: ${detail}"
        log.error(errorMsg)
        return ServiceResponse.error(errorMsg)
    }
}
