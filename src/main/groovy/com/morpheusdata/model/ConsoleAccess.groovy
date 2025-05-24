package com.morpheusdata.model

/**
 * Minimal representation of a console access descriptor used for tests.
 * This is a simplified stand-in for the real Morpheus model class.
 */
class ConsoleAccess {
    String consoleType
    String targetHost
    Integer port
    String ticket
    String webSocketUrl

    Map toMap() {
        [consoleType: consoleType, targetHost: targetHost, port: port,
         ticket: ticket, webSocketUrl: webSocketUrl]
    }
}
