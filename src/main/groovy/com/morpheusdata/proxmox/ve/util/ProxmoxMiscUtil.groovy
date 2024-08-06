package com.morpheusdata.proxmox.ve.util

import com.jcraft.jsch.Channel
import com.jcraft.jsch.ChannelSftp
import com.jcraft.jsch.JSch
import com.jcraft.jsch.Session
import groovy.util.logging.Slf4j


@Slf4j
class ProxmoxMiscUtil {


    static void sftpUpload(String host, int port, String username, String password, String localPath, String destDir, String privateKeyPath) {
        JSch jsch = new JSch()
        Session session = null
        Channel channel = null
        ChannelSftp channelSftp = null

        try {
            // Set up the private key if provided
            if (privateKeyPath) {
                jsch.addIdentity(privateKeyPath)
            }

            // Open a session to the remote server
            session = jsch.getSession(username, host, port)
            session.setConfig("StrictHostKeyChecking", "no") // Not recommended for production
            session.setPassword(password)
            session.connect()

            // Open the SFTP channel
            channel = session.openChannel("sftp")
            channel.connect()
            channelSftp = channel as ChannelSftp

            // Upload file
            String fileName = new File(localPath).getName()
            String remoteFilePath = destDir.endsWith("/") ? destDir + fileName : destDir + "/" + fileName;
            long startTime = System.currentTimeMillis();
            channelSftp.put(localPath, remoteFilePath)
            long endTime = System.currentTimeMillis()
            long duration = (endTime - startTime) / 1000
            log.debug("File uploaded: $localPath to $remoteFilePath in $duration seconds")

        } catch (Exception e) {
            e.printStackTrace()
        } finally {
            if (channelSftp != null) {
                channelSftp.exit()
            }
            if (channel != null) {
                channel.disconnect()
            }
            if (session != null) {
                session.disconnect()
            }
        }
    }


    static void sftpCreateFile(String host, int port, String username, String password, String destFilePath, String content, String privateKeyPath) {
        JSch jsch = new JSch()
        Session session = null
        Channel channel = null
        ChannelSftp channelSftp = null

        try {
            // Set up the private key if provided
            if (privateKeyPath) {
                jsch.addIdentity(privateKeyPath)
            }

            // Open a session to the remote server
            session = jsch.getSession(username, host, port)
            session.setConfig("StrictHostKeyChecking", "no") // Not recommended for production
            session.setPassword(password)
            session.connect()

            // Open the SFTP channel
            channel = session.openChannel("sftp")
            channel.connect()
            channelSftp = channel as ChannelSftp

            // Create and write to the file
            long startTime = System.currentTimeMillis();
            ByteArrayInputStream inputStream = new ByteArrayInputStream(content.getBytes("UTF-8"))
            channelSftp.put(inputStream, destFilePath)
            long endTime = System.currentTimeMillis()
            long duration = (endTime - startTime) / 1000
            log.debug("File created: $destFilePath in $duration seconds")

        } catch (Exception e) {
            e.printStackTrace()
        } finally {
            if (channelSftp != null) {
                channelSftp.exit()
            }
            if (channel != null) {
                channel.disconnect()
            }
            if (session != null) {
                session.disconnect()
            }
        }
    }




}
