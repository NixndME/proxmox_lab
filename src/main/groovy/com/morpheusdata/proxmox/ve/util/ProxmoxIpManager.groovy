package com.morpheusdata.proxmox.ve.util

import groovy.util.logging.Slf4j
import java.util.*

@Slf4j
class ProxmoxIpManager {
    private static final Set<String> reservedIps = Collections.synchronizedSet(new HashSet<String>())

    /**
     * Attempts to reserve an IP address. Returns true if the IP was reserved,
     * false if it was already reserved.
     */
    static boolean reserveIp(String ip) {
        if(!ip)
            return false
        synchronized(reservedIps) {
            if(reservedIps.contains(ip)) {
                return false
            }
            reservedIps.add(ip)
            log.debug("Reserved IP address ${ip}")
            return true
        }
    }

    /**
     * Releases a previously reserved IP address.
     */
    static void releaseIp(String ip) {
        if(!ip)
            return
        reservedIps.remove(ip)
        log.debug("Released IP address ${ip}")
    }

    /**
     * Checks if an IP address is already reserved.
     */
    static boolean isReserved(String ip) {
        if(!ip)
            return false
        reservedIps.contains(ip)
    }

    /**
     * Returns a copy of all reserved IP addresses.
     */
    static Set<String> getReservedIps() {
        return Collections.unmodifiableSet(new HashSet<String>(reservedIps))
    }
}
