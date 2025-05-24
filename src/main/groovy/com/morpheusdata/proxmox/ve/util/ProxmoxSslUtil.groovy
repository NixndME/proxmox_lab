package com.morpheusdata.proxmox.ve.util

import groovy.util.logging.Slf4j
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory
import java.security.KeyStore

/**
 * Utility for configuring SSL handling for Proxmox API interactions.
 *
 * The behaviour can be adjusted with environment variables:
 *  - PROXMOX_IGNORE_SSL (true/false) : when true SSL certificate validation is disabled.
 *  - PROXMOX_TRUST_STORE : path to a Java trust store used for validation.
 *  - PROXMOX_TRUST_STORE_PASSWORD : password for the trust store if required.
 */
@Slf4j
class ProxmoxSslUtil {
    /**
     * Determines if SSL validation should be ignored based on the
     * PROXMOX_IGNORE_SSL environment variable. Defaults to false.
     */
    static final boolean IGNORE_SSL = (System.getenv('PROXMOX_IGNORE_SSL') ?: 'false')
            .toString().toLowerCase() in ['1','true','yes']

    private static boolean sslContextConfigured = false

    /**
     * Configures a JVM wide SSLContext if PROXMOX_TRUST_STORE is set. This allows
     * usage of a custom trust store for SSL validation.
     */
    static synchronized void configureSslContextIfNeeded() {
        if (sslContextConfigured) {
            return
        }
        String trustStorePath = System.getenv('PROXMOX_TRUST_STORE')
        if (trustStorePath) {
            try {
                char[] password = System.getenv('PROXMOX_TRUST_STORE_PASSWORD')?.toCharArray()
                KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType())
                trustStore.load(new FileInputStream(trustStorePath), password)
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
                tmf.init(trustStore)
                SSLContext context = SSLContext.getInstance('TLS')
                context.init(null, tmf.trustManagers, null)
                SSLContext.setDefault(context)
                log.info("Configured SSL context using trust store at ${trustStorePath}")
            } catch(Exception e) {
                log.error("Failed to configure SSL context using trust store ${trustStorePath}: ${e.message}", e)
            }
        }
        sslContextConfigured = true
    }
}
