package com.morpheusdata.proxmox.ve

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.backup.BackupJobProvider
import com.morpheusdata.core.backup.MorpheusBackupProvider
import groovy.util.logging.Slf4j

@Slf4j
class ProxmoxBackupProvider extends MorpheusBackupProvider {

    ProxmoxBackupProvider(ProxmoxVePlugin plugin, MorpheusContext morpheusContext) {
        super(plugin, morpheusContext)

        ProxmoxBackupTypeProvider backupTypeProvider = new ProxmoxBackupTypeProvider(plugin, morpheus)
        plugin.registerProvider(backupTypeProvider)
        // TODO: Verify scope, "proxmox" should match the cloud code or a specific Proxmox VM provision type code.
        addScopedProvider(backupTypeProvider, "proxmox", null) 
    }
}
