package com.morpheusdata.proxmox.ve

import com.morpheusdata.proxmox.ve.sync.DatastoreSync
import com.morpheusdata.proxmox.ve.sync.HostSync
import com.morpheusdata.proxmox.ve.sync.NetworkSync
import com.morpheusdata.proxmox.ve.sync.VirtualImageLocationSync
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.providers.CloudProvider
import com.morpheusdata.core.providers.ProvisionProvider
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.CloudFolder
import com.morpheusdata.model.CloudPool
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.ComputeServerType
import com.morpheusdata.model.Datastore
import com.morpheusdata.model.Icon
import com.morpheusdata.model.Network
import com.morpheusdata.model.NetworkSubnetType
import com.morpheusdata.model.NetworkType
import com.morpheusdata.model.OptionType
import com.morpheusdata.model.PlatformType
import com.morpheusdata.model.StorageControllerType
import com.morpheusdata.model.StorageVolumeType
import com.morpheusdata.model.projection.ComputeServerIdentityProjection
import com.morpheusdata.request.ValidateCloudRequest
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.proxmox.ve.util.ProxmoxApiComputeUtil
import com.morpheusdata.proxmox.ve.sync.VMSync
import com.morpheusdata.proxmox.ve.util.ProxmoxApiUtil
import com.morpheusdata.proxmox.ve.util.ProxmoxSslUtil
import com.morpheusdata.model.ConsoleAccess
import org.apache.http.entity.ContentType
import java.net.URI
import java.net.URLEncoder
import groovy.util.logging.Slf4j


@Slf4j
class ProxmoxVeCloudProvider implements CloudProvider {
	public static final String CLOUD_PROVIDER_CODE = 'proxmox-ve.cloud'

	protected MorpheusContext context
	protected ProxmoxVePlugin plugin

	public ProxmoxVeCloudProvider(ProxmoxVePlugin plugin, MorpheusContext ctx) {
		this.@plugin = plugin
		this.@context = ctx
	}

	/**
	 * Grabs the description for the CloudProvider
	 * @return String
	 */
	@Override
	String getDescription() {
		return 'Proxmox Virtual Environment Integration'
	}

	/**
	 * Returns the Cloud logo for display when a user needs to view or add this cloud. SVGs are preferred.
	 * @since 0.13.0
	 * @return Icon representation of assets stored in the src/assets of the project.
	 */
	@Override
	Icon getIcon() {
		return new Icon(path:'proxmox-full-lockup-color.svg', darkPath:'proxmox-full-lockup-inverted-color.svg')
	}

	/**
	 * Returns the circular Cloud logo for display when a user needs to view or add this cloud. SVGs are preferred.
	 * @since 0.13.6
	 * @return Icon
	 */
	@Override
	Icon getCircularIcon() {
		return new Icon(path:'proxmox-logo-stacked-color.svg', darkPath:'proxmox-logo-stacked-inverted-color.svg')
	}

	/**
	 * Provides a Collection of OptionType inputs that define the required input fields for defining a cloud integration
	 * @return Collection of OptionType
	 */
	@Override
	Collection<OptionType> getOptionTypes() {
		Collection<OptionType> options = []

		options << new OptionType(
				name: 'Proxmox API URL',
				code: 'proxmox-url',
				displayOrder: 0,
				fieldContext: 'domain',
				fieldLabel: 'Proxmox API URL',
				fieldCode: 'gomorpheus.optiontype.serviceUrl',
				fieldName: 'serviceUrl',
				inputType: OptionType.InputType.TEXT,
				required: true,
				defaultValue: ""
		)

		options << new OptionType(
				code: 'proxmox-credential',
				inputType: OptionType.InputType.CREDENTIAL,
				name: 'Credentials',
				fieldName: 'type',
				fieldLabel: 'Credentials',
				fieldContext: 'credential',
				required: true,
				defaultValue: 'local',
				displayOrder: 1,
				optionSource: 'credentials',
				config: '{"credentialTypes":["username-password"]}'
		)
		options << new OptionType(
				name: 'User Name',
				code: 'proxmox-username',
				displayOrder: 2,
				fieldContext: 'config',
				fieldLabel: 'User Name',
				fieldCode: 'gomorpheus.optiontype.UserName',
				fieldName: 'username',
				inputType: OptionType.InputType.TEXT,
				localCredential: true,
				required: true
		)
		options << new OptionType(
				name: 'Password',
				code: 'proxmox-password',
				displayOrder: 3,
				fieldContext: 'config',
				fieldLabel: 'Password',
				fieldCode: 'gomorpheus.optiontype.Password',
				fieldName: 'password',
				inputType: OptionType.InputType.PASSWORD,
				localCredential: true,
				required: true
		)
                options << new OptionType(
                                name: 'Enable Console Access',
                                code: 'proxmox-enable-console',
                                displayOrder: 4,
                                fieldContext: 'config',
                                fieldLabel: 'Enable Console Access',
                                fieldName: 'enableConsoleAccess',
                                inputType: OptionType.InputType.CHECKBOX,
                                defaultValue: false,
                                required: false
                )
/*		options << new OptionType(
				name: 'Proxmox Token',
				code: 'proxmox-token',
				displayOrder: 4,
				fieldContext: 'config',
				fieldLabel: 'Proxmox Token',
				fieldCode: 'gomorpheus.optiontype.Token',
				fieldName: 'token',
				inputType: OptionType.InputType.PASSWORD,
				localCredential: false,
				required: true
		)
*/
		return options
	}
	/**
	 * Grabs available provisioning providers related to the target Cloud Plugin. Some clouds have multiple provisioning
	 * providers or some clouds allow for service based providers on top like (Docker or Kubernetes).
	 * @return Collection of ProvisionProvider
	 */
	@Override
	Collection<ProvisionProvider> getAvailableProvisionProviders() {
	    return this.@plugin.getProvidersByType(ProvisionProvider) as Collection<ProvisionProvider>
	}

	/**
	 * Grabs available backup providers related to the target Cloud Plugin.
	 * @return Collection of BackupProvider
	 */
	@Override
	Collection<BackupProvider> getAvailableBackupProviders() {
		Collection<BackupProvider> providers = []
		return providers
	}

	/**
	 * Provides a Collection of {@link NetworkType} related to this CloudProvider
	 * @return Collection of NetworkType
	 */
	@Override
	Collection<NetworkType> getNetworkTypes() {

		NetworkType bridgeNetwork = new NetworkType([
				code              : 'proxmox-ve-bridge-network',
				externalType      : 'LinuxBridge',
				cidrEditable      : true,
				dhcpServerEditable: true,
				dnsEditable       : true,
				gatewayEditable   : true,
                                vlanIdEditable    : true,
				canAssignPool     : true,
				name              : 'Proxmox VE Bridge Network',
				hasNetworkServer  : true,
				creatable: true
		])

		return [bridgeNetwork]
	}

	/**
	 * Provides a Collection of {@link NetworkSubnetType} related to this CloudProvider
	 * @return Collection of NetworkSubnetType
	 */
	@Override
	Collection<NetworkSubnetType> getSubnetTypes() {
		Collection<NetworkSubnetType> subnets = []
		return subnets
	}

	/**
	 * Provides a Collection of {@link StorageVolumeType} related to this CloudProvider
	 * @return Collection of StorageVolumeType
	 */
	@Override
	Collection<StorageVolumeType> getStorageVolumeTypes() {
		Collection<StorageVolumeType> volumeTypes = []

		volumeTypes << new StorageVolumeType(
				name: "Proxmox VM Generic Volume Type",
				code: "proxmox.vm.generic.volume.type",
				displayOrder: 0,
				editable: true,
				resizable: true
		)

		return volumeTypes
	}

	/**
	 * Provides a Collection of {@link StorageControllerType} related to this CloudProvider
	 * @return Collection of StorageControllerType
	 */
	@Override
	Collection<StorageControllerType> getStorageControllerTypes() {
		Collection<StorageControllerType> controllerTypes = []
		return controllerTypes
	}

	/**
	 * Grabs all {@link ComputeServerType} objects that this CloudProvider can represent during a sync or during a provision.
	 * @return collection of ComputeServerType
	 */
	@Override
	Collection<ComputeServerType> getComputeServerTypes() {
		Collection<ComputeServerType> serverTypes = []

                serverTypes << new ComputeServerType (
                                name: 'Proxmox VE Node',
                                code: 'proxmox-ve-node',
                                description: 'Proxmox VE Node',
                                vmHypervisor: true,
                                controlPower: true,
				reconfigureSupported: false,
				externalDelete: false,
				hasAutomation: false,
				agentType: ComputeServerType.AgentType.none,
				platform: PlatformType.linux,
				managed: true,
				provisionTypeCode: 'proxmox-provision-provider',
				nodeType: 'proxmox-node'
		)
		serverTypes << new ComputeServerType (
				name: 'Proxmox VE VM',
				code: 'proxmox-qemu-vm',
				description: 'Proxmox VE Qemu VM',
				vmHypervisor: false,
				controlPower: true,
				reconfigureSupported: false,
				externalDelete: false,
				hasAutomation: true,
				agentType: ComputeServerType.AgentType.guest,
				platform: PlatformType.linux,
				managed: true,
				provisionTypeCode: 'proxmox-provision-provider',
				nodeType: 'morpheus-vm-node'
		)
		serverTypes << new ComputeServerType (
				name: 'Proxmox VE VM',
				code: 'proxmox-qemu-vm-unmanaged',
				description: 'Proxmox VE Qemu VM',
				vmHypervisor: false,
				controlPower: true,
				reconfigureSupported: false,
				externalDelete: false,
				hasAutomation: true,
				agentType: ComputeServerType.AgentType.none,
				platform: PlatformType.linux,
				managed: false,
				provisionTypeCode: 'proxmox-provision-provider',
				nodeType: 'unmanaged'
		)
		return serverTypes
	}


	/**
	 * Validates the submitted cloud information to make sure it is functioning correctly.
	 * If a {@link ServiceResponse} is not marked as successful then the validation results will be
	 * bubbled up to the user.
	 * @param cloudInfo cloud
	 * @param validateCloudRequest Additional validation information
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse validate(Cloud cloudInfo, ValidateCloudRequest validateCloudRequest) {	
		log.debug("validate: {}", cloudInfo)
		try {
			if(!cloudInfo) {
				return new ServiceResponse(success: false, msg: 'No cloud found')
			}

			def username, password
			def baseUrl = cloudInfo.serviceUrl
			log.debug("Cloud Service URL: $baseUrl")

			// Provided creds vs. Infra > Trust creds
			if (validateCloudRequest.credentialType == 'username-password') {
				log.debug("Adding cloud with username-password credentialType")
				username = validateCloudRequest.credentialUsername ?: cloudInfo.serviceUsername
				password = validateCloudRequest.credentialPassword ?: cloudInfo.servicePassword
			} else if (validateCloudRequest.credentialType == 'local') {
				log.debug("Adding cloud with local credentialType")
				username = cloudInfo.getConfigMap().get("username")
				password = cloudInfo.getConfigMap().get("password")
			} else {
				return new ServiceResponse(success: false, msg: "Unknown credential source type $validateCloudRequest.credentialType")
			}

			// Integration needs creds and a base URL
			if (username?.length() < 1 ) {
				return new ServiceResponse(success: false, msg: 'Enter a username.')
			} else if (password?.length() < 1) {
				return new ServiceResponse(success: false, msg: 'Enter a password.')
			} else if (cloudInfo.serviceUrl.length() < 1) {
				return new ServiceResponse(success: false, msg: 'Enter a base url.')
			}

			// Setup token get using util class
			log.debug("Cloud Validation: Attempting authentication to populate access token and csrf token.")
			def tokenTest = ProxmoxApiComputeUtil.getApiV2Token([username: username, password: password, apiUrl: baseUrl, v2basePath: ProxmoxVePlugin.V2_BASE_PATH])
			if (tokenTest.success) {
				return new ServiceResponse(success: true, msg: 'Cloud connection validated using provided credentials and URL...')
			} else {
				return new ServiceResponse(success: false, msg: 'Unable to validate cloud connection using provided credentials and URL')
			}
		} catch(e) {
			log.error('Error validating cloud', e)
			return new ServiceResponse(success: false, msg: "Error validating cloud ${e}")
		}
	}

	/**
	 * Called when a Cloud From Morpheus is first saved. This is a hook provided to take care of initial state
	 * assignment that may need to take place.
	 * @param cloudInfo instance of the cloud object that is being initialized.
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse initializeCloud(Cloud cloudInfo) {
		
		plugin.getNetworkProvider().initializeProvider(cloudInfo)

		refresh(cloudInfo)
		return ServiceResponse.success()
	}

	/**
	 * Zones/Clouds are refreshed periodically by the Morpheus Environment. This includes things like caching of brownfield
	 * environments and resources such as Networks, Datastores, Resource Pools, etc.
	 * @param cloudInfo cloud
	 * @return ServiceResponse. If ServiceResponse.success == true, then Cloud status will be set to Cloud.Status.ok. If
	 * ServiceResponse.success == false, the Cloud status will be set to ServiceResponse.data['status'] or Cloud.Status.error
	 * if not specified. So, to indicate that the Cloud is offline, return `ServiceResponse.error('cloud is not reachable', null, [status: Cloud.Status.offline])`
	 */
	@Override
	ServiceResponse refresh(Cloud cloudInfo) {

		log.debug("Refresh triggered, service url is: " + cloudInfo.serviceUrl)
		HttpApiClient client = new HttpApiClient()
		try {
			log.debug("Synchronizing hosts, datastores, networks, VMs and virtual images...")
			(new HostSync(plugin, cloudInfo, client)).execute()
			(new DatastoreSync(plugin, cloudInfo, client)).execute()
			(new NetworkSync(plugin, cloudInfo, client)).execute()
			(new VMSync(plugin, cloudInfo, client, this)).execute()
			(new VirtualImageLocationSync(plugin, cloudInfo, client, this)).execute()

		} catch (e) {
			log.error("refresh cloud error: ${e}", e)
			return ServiceResponse.error("refresh cloud error: ${e}")
		} finally {
			if(client) {
				client.shutdownClient()
			}
		}
		return ServiceResponse.success()
	}

	/**
	 * Zones/Clouds are refreshed periodically by the Morpheus Environment. This includes things like caching of brownfield
	 * environments and resources such as Networks, Datastores, Resource Pools, etc. This represents the long term sync method that happens
	 * daily instead of every 5-10 minute cycle
	 * @param cloudInfo cloud
	 */
	@Override
	void refreshDaily(Cloud cloudInfo) {

		log.debug("Synchronizing hosts, datastores, networks, VMs and virtual images daily...")
		def refreshResults = refresh(cloudInfo)

		if(refreshResults.success) {
			cloudInfo.status = Cloud.Status.ok
		} else {
			log.debug("Error during daily cloud refresh!")
			cloudInfo.status = Cloud.Status.offline
		}

		context.async.cloud.save(cloudInfo).subscribe().dispose()
	}

	/**
	 * Called when a Cloud From Morpheus is removed. This is a hook provided to take care of cleaning up any state.
	 * @param cloudInfo instance of the cloud object that is being removed.
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse deleteCloud(Cloud cloudInfo) {
		log.debug("Cleanup (deleteCloud) triggered, service url is: " + cloudInfo.serviceUrl)
		HttpApiClient client = new HttpApiClient()
		try {
			(new VirtualImageLocationSync(plugin, cloudInfo, client, this)).clean()
			return ServiceResponse.success()
		} catch (e) {
			log.error("Error during deleteCloud for cloud ${cloudInfo.id} (${cloudInfo.name}): ${e.message}", e)
			return ServiceResponse.error("Error during cloud cleanup: ${e.message}")
		} finally {
			client?.shutdownClient()
		}
	}

	/**
	 * Returns whether the cloud supports {@link CloudPool}
	 * @return Boolean
	 */
	@Override
	Boolean hasComputeZonePools() {
		return false
	}

	/**
	 * Returns whether a cloud supports {@link Network}
	 * @return Boolean
	 */
	@Override
	Boolean hasNetworks() {
		return true
	}

	/**
	 * Returns whether a cloud supports {@link CloudFolder}
	 * @return Boolean
	 */
	@Override
	Boolean hasFolders() {
		return false
	}

	/**
	 * Returns whether a cloud supports {@link Datastore}
	 * @return Boolean
	 */
	@Override
	Boolean hasDatastores() {
		return true
	}

	/**
	 * Returns whether a cloud supports bare metal VMs
	 * @return Boolean
	 */
	@Override
	Boolean hasBareMetal() {
		return false
	}

	/**
	 * Indicates if the cloud supports cloud-init. Returning true will allow configuration of the Cloud
	 * to allow installing the agent remotely via SSH /WinRM or via Cloud Init
	 * @return Boolean
	 */
	@Override
	Boolean hasCloudInit() {
		return true
	}

	/**
	 * Indicates if the cloud supports the distributed worker functionality
	 * @return Boolean
	 */
        @Override
        Boolean supportsDistributedWorker() {
                return false
        }


	/**
	 * Called when a server should be started. Returning a response of success will cause corresponding updates to usage
	 * records, result in the powerState of the computeServer to be set to 'on', and related instances set to 'running'
	 * @param computeServer server to start
	 * @return ServiceResponse
	 */
	@Override
        ServiceResponse startServer(ComputeServer computeServer) {
                HttpApiClient client = new HttpApiClient()
                try {
                        Map authConfig = plugin.getAuthConfig(computeServer.cloud)
                        if(computeServer.serverType == 'hypervisor') {
                                String nodeName = computeServer.externalId
                                return ProxmoxApiComputeUtil.startNode(client, authConfig, nodeName)
                        }
                        String vmId = computeServer.externalId
                        String nodeName = computeServer.parentServer?.name

			if (!vmId) {
				return ServiceResponse.error("Missing externalId (VM ID) for server ${computeServer.name}")
			}
			if (!nodeName) {
				// Attempt to find node if parentServer is not set (e.g. for unmanaged VMs not fully synced)
				log.warn("parentServer.name is null for ComputeServer ${computeServer.id} (${computeServer.name}). Attempting to find node for VM ID ${vmId}.")
				def vmsList = ProxmoxApiComputeUtil.listVMs(client, authConfig)?.data
				def foundVm = vmsList?.find { it.vmid == vmId }
				if (foundVm?.node) {
					nodeName = foundVm.node
					log.info("Found node '${nodeName}' for VM ID ${vmId} by querying Proxmox API.")
				} else {
					return ServiceResponse.error("Missing nodeName (parentServer.name) for server ${computeServer.name} and could not find it via API.")
				}
			}

			log.info("Attempting to start VM ${vmId} on node ${nodeName}")
			ServiceResponse response = ProxmoxApiComputeUtil.startVM(client, authConfig, nodeName, vmId)
			if (response.success) {
				log.info("Successfully started VM ${vmId} on node ${nodeName}")
			} else {
				log.error("Failed to start VM ${vmId} on node ${nodeName}: ${response.msg}")
			}
			return response
		} catch (e) {
			log.error("Error performing start on VM ${computeServer.externalId}: ${e.message}", e)
			return ServiceResponse.error("Error performing start on VM ${computeServer.externalId}: ${e.message}")
		} finally {
			client?.shutdownClient()
		}
	}

	/**
	 * Called when a server should be stopped. Returning a response of success will cause corresponding updates to usage
	 * records, result in the powerState of the computeServer to be set to 'off', and related instances set to 'stopped'
	 * @param computeServer server to stop
	 * @return ServiceResponse
	 */
	@Override
        ServiceResponse stopServer(ComputeServer computeServer) {
                HttpApiClient client = new HttpApiClient()
                try {
                        Map authConfig = plugin.getAuthConfig(computeServer.cloud)
                        if(computeServer.serverType == 'hypervisor') {
                                String nodeName = computeServer.externalId
                                return ProxmoxApiComputeUtil.shutdownNode(client, authConfig, nodeName)
                        }
                        String vmId = computeServer.externalId
                        String nodeName = computeServer.parentServer?.name

			if (!vmId) {
				return ServiceResponse.error("Missing externalId (VM ID) for server ${computeServer.name}")
			}
			if (!nodeName) {
				log.warn("parentServer.name is null for ComputeServer ${computeServer.id} (${computeServer.name}). Attempting to find node for VM ID ${vmId}.")
				def vmsList = ProxmoxApiComputeUtil.listVMs(client, authConfig)?.data
				def foundVm = vmsList?.find { it.vmid == vmId }
				if (foundVm?.node) {
					nodeName = foundVm.node
					log.info("Found node '${nodeName}' for VM ID ${vmId} by querying Proxmox API.")
				} else {
					return ServiceResponse.error("Missing nodeName (parentServer.name) for server ${computeServer.name} and could not find it via API.")
				}
			}

			log.info("Attempting to gracefully shutdown VM ${vmId} on node ${nodeName}")
			// Using shutdownVM for graceful shutdown
			ServiceResponse response = ProxmoxApiComputeUtil.shutdownVM(client, authConfig, nodeName, vmId)
			if (response.success) {
				log.info("Successfully initiated shutdown for VM ${vmId} on node ${nodeName}")
			} else {
				log.error("Failed to shutdown VM ${vmId} on node ${nodeName}: ${response.msg}")
			}
			return response
		} catch (e) {
			log.error("Error performing stop (shutdown) on VM ${computeServer.externalId}: ${e.message}", e)
			return ServiceResponse.error("Error performing stop (shutdown) on VM ${computeServer.externalId}: ${e.message}")
		} finally {
			client?.shutdownClient()
		}
	}

	/**
	 * Called when a server should be deleted from the Cloud.
	 * @param computeServer server to delete
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse deleteServer(ComputeServer computeServer) {
		HttpApiClient client = new HttpApiClient()
		try {
			Map authConfig = plugin.getAuthConfig(computeServer.cloud)
			String vmId = computeServer.externalId
			String nodeName = computeServer.parentServer?.name

			if (!vmId) {
				return ServiceResponse.error("Missing externalId (VM ID) for server ${computeServer.name}")
			}
			if (!nodeName) {
				log.warn("parentServer.name is null for ComputeServer ${computeServer.id} (${computeServer.name}). Attempting to find node for VM ID ${vmId}.")
				def vmsList = ProxmoxApiComputeUtil.listVMs(client, authConfig)?.data
				def foundVm = vmsList?.find { it.vmid == vmId }
				if (foundVm?.node) {
					nodeName = foundVm.node
					log.info("Found node '${nodeName}' for VM ID ${vmId} by querying Proxmox API.")
				} else {
					return ServiceResponse.error("Missing nodeName (parentServer.name) for server ${computeServer.name} and could not find it via API.")
				}
			}

			log.info("Attempting to stop VM ${vmId} on node ${nodeName} before deletion.")
			ServiceResponse stopResponse = ProxmoxApiComputeUtil.stopVM(client, authConfig, nodeName, vmId)
			if (stopResponse.success) {
				log.info("Successfully stopped VM ${vmId} on node ${nodeName}. Waiting for 5 seconds before deletion.")
				sleep(5000) // Wait for VM to stop, similar to ProxmoxVeProvisionProvider.removeWorkload
			} else {
				// Log the error but proceed with deletion attempt if stop fails, as VM might already be stopped or Proxmox might handle it.
				log.warn("Failed to stop VM ${vmId} on node ${nodeName} before deletion: ${stopResponse.msg}. Proceeding with deletion attempt.")
			}

			log.info("Attempting to delete VM ${vmId} on node ${nodeName}")
			ServiceResponse deleteResponse = ProxmoxApiComputeUtil.destroyVM(client, authConfig, nodeName, vmId)
			if (deleteResponse.success) {
				log.info("Successfully deleted VM ${vmId} on node ${nodeName}")
			} else {
				log.error("Failed to delete VM ${vmId} on node ${nodeName}: ${deleteResponse.msg}")
			}
			return deleteResponse
		} catch (e) {
			log.error("Error performing delete on VM ${computeServer.externalId}: ${e.message}", e)
			return ServiceResponse.error("Error performing delete on VM ${computeServer.externalId}: ${e.message}")
		} finally {
			client?.shutdownClient()
		}
	}

	/**
	 * Grabs the singleton instance of the provisioning provider based on the code defined in its implementation.
	 * Typically Providers are singleton and instanced in the {@link Plugin} class
	 * @param providerCode String representation of the provider short code
	 * @return the ProvisionProvider requested
	 */
	@Override
	ProvisionProvider getProvisionProvider(String providerCode) {
		return getAvailableProvisionProviders().find { it.code == providerCode }
	}

	/**
	 * Returns the default provision code for fetching a {@link ProvisionProvider} for this cloud.
	 * This is only really necessary if the provision type code is the exact same as the cloud code.
	 * @return the provision provider code
	 */
	@Override
	String getDefaultProvisionTypeCode() {
		return ProxmoxVeProvisionProvider.PROVISION_PROVIDER_CODE
	}

	/**
	 * Returns the Morpheus Context for interacting with data stored in the Main Morpheus Application
	 * @return an implementation of the MorpheusContext for running Future based rxJava queries
	 */
	@Override
	MorpheusContext getMorpheus() {
		return this.@context
	}

	/**
	 * Returns the instance of the Plugin class that this provider is loaded from
	 * @return Plugin class contains references to other providers
	 */
	@Override
	Plugin getPlugin() {
		return this.@plugin
	}

	/**
	 * A unique shortcode used for referencing the provided provider. Make sure this is going to be unique as any data
	 * that is seeded or generated related to this provider will reference it by this code.
	 * @return short code string that should be unique across all other plugin implementations.
	 */
	@Override
	String getCode() {
		return CLOUD_PROVIDER_CODE
	}

	/**
	 * Provides the provider name for reference when adding to the Morpheus Orchestrator
	 * NOTE: This may be useful to set as an i18n key for UI reference and localization support.
	 *
	 * @return either an English name of a Provider or an i18n based key that can be scanned for in a properties file.
	 */
	@Override
	String getName() {
		return 'Proxmox VE'
	}

	/**
	 * Adds a new disk to the specified ComputeServer.
	 * This method is intended to be called to provision a new disk directly on the Proxmox VM.
	 *
	 * @param server The ComputeServer (VM) to add the disk to.
	 * @param storageName The name of the Proxmox storage where the new disk will be created (e.g., 'local-lvm').
	 * @param diskSizeGB The size of the new disk in Gigabytes.
	 * @param diskType The Proxmox disk type (e.g., 'scsi', 'sata', 'ide', 'virtio'). This determines the controller/bus.
	 * @return ServiceResponse indicating success or failure, and potentially a taskId from Proxmox.
	 */
	ServiceResponse addDiskToServer(ComputeServer server, String storageName, Integer diskSizeGB, String diskType) {
		log.info("ProxmoxVeCloudProvider.addDiskToServer called for server: ${server?.id} (${server?.name}), storage: ${storageName}, size: ${diskSizeGB}GB, type: ${diskType}")
		HttpApiClient client = new HttpApiClient()
		try {
			// Basic input validation
			if (server == null) {
				return ServiceResponse.error("ComputeServer cannot be null.")
			}
			if (server.cloud == null) {
				return ServiceResponse.error("ComputeServer cloud information is missing.")
			}
			if (storageName == null || storageName.isEmpty()) {
				return ServiceResponse.error("Storage name cannot be empty.")
			}
			if (diskSizeGB == null || diskSizeGB <= 0) {
				return ServiceResponse.error("Disk size must be a positive integer.")
			}
			if (diskType == null || diskType.isEmpty()) {
				return ServiceResponse.error("Disk type (bus) cannot be empty.")
			}
			List<String> supportedDiskTypes = ['scsi', 'sata', 'ide', 'virtio']
			if (!supportedDiskTypes.contains(diskType.toLowerCase())) {
				return ServiceResponse.error("Unsupported diskType: ${diskType}. Must be one of ${supportedDiskTypes.join(', ')}.")
			}

			Map authConfig = plugin.getAuthConfig(server.cloud)
			String vmId = server.externalId
			String nodeName = server.parentServer?.name

			if (vmId == null || vmId.isEmpty()) {
				return ServiceResponse.error("Missing externalId (VM ID) for server ${server.name}")
			}

			// Fallback for nodeName if parentServer is not set
			if (nodeName == null || nodeName.isEmpty()) {
				log.warn("parentServer.name is null for ComputeServer ${server.id} (${server.name}). Attempting to find node for VM ID ${vmId}.")
				// Use a temporary client for this internal lookup if the main client is used later for the actual operation
				HttpApiClient tempClient = new HttpApiClient()
				try {
					def vmsList = ProxmoxApiComputeUtil.listVMs(tempClient, authConfig)?.data
					def foundVm = vmsList?.find { it.vmid.toString() == vmId }
					if (foundVm?.node) {
						nodeName = foundVm.node
						log.info("Found node '${nodeName}' for VM ID ${vmId} by querying Proxmox API.")
					} else {
						return ServiceResponse.error("Missing nodeName (parentServer.name) for server ${server.name} and could not find it via API.")
					}
				} finally {
					tempClient?.shutdownClient()
				}
			}

			log.info("Attempting to add disk to VM ${vmId} on node ${nodeName}: Storage=${storageName}, Size=${diskSizeGB}GB, Type=${diskType}")
			ServiceResponse response = ProxmoxApiComputeUtil.addVMDisk(client, authConfig, nodeName, vmId, storageName, diskSizeGB, diskType)

			if (response.success) {
				log.info("Successfully initiated add disk for VM ${vmId} on node ${nodeName}. Response: ${response.msg}")
			} else {
				log.error("Failed to add disk to VM ${vmId} on node ${nodeName}: ${response.msg}")
			}
			return response

		} catch (e) {
			log.error("Error in addDiskToServer for VM ${server?.externalId}: ${e.message}", e)
			return ServiceResponse.error("Error adding disk to server ${server?.name}: ${e.message}")
		} finally {
			client?.shutdownClient()
		}
	}

	/**
	 * Adds a new network interface to the specified ComputeServer (VM).
	 *
	 * @param server The ComputeServer (VM) to add the network interface to.
	 * @param bridgeName The name of the Proxmox bridge (e.g., 'vmbr0').
	 * @param model The network card model (e.g., 'e1000', 'virtio', 'rtl8139').
	 * @param vlanTag (Optional) The VLAN ID. If null or empty, it's omitted.
	 * @param firewallEnabled (Optional) Boolean to enable/disable Proxmox firewall on this interface. Defaults to false if null.
	 * @return ServiceResponse indicating success or failure, and potentially a taskId from Proxmox.
	 */
	ServiceResponse addNetworkInterfaceToServer(ComputeServer server, String bridgeName, String model, String vlanTag, Boolean firewallEnabled) {
		log.info("ProxmoxVeCloudProvider.addNetworkInterfaceToServer called for server: ${server?.id} (${server?.name}), bridge: ${bridgeName}, model: ${model}, vlan: ${vlanTag}, firewall: ${firewallEnabled}")
		HttpApiClient client = new HttpApiClient()
		try {
			// Basic input validation
			if (server == null) {
				return ServiceResponse.error("ComputeServer cannot be null.")
			}
			if (server.cloud == null) {
				return ServiceResponse.error("ComputeServer cloud information is missing.")
			}
			if (bridgeName == null || bridgeName.trim().isEmpty()) {
				return ServiceResponse.error("Bridge name cannot be empty.")
			}
			if (model == null || model.trim().isEmpty()) {
				return ServiceResponse.error("Network card model cannot be empty.")
			}
			// Proxmox supports various models, e.g., e1000, virtio, rtl8139, vmxnet3. Add more if needed or rely on Proxmox to validate.
			List<String> knownModels = ['e1000', 'e1000-82545em', 'virtio', 'rtl8139', 'vmxnet3', 'i82551', 'i82557b', 'i82559er']
			if (!knownModels.contains(model.toLowerCase())) {
				log.warn("Unknown network model specified: ${model}. Proxmox will ultimately validate this.")
			}


			Map authConfig = plugin.getAuthConfig(server.cloud)
			String vmId = server.externalId
			String nodeName = server.parentServer?.name

			if (vmId == null || vmId.isEmpty()) {
				return ServiceResponse.error("Missing externalId (VM ID) for server ${server.name}")
			}

			// Fallback for nodeName if parentServer is not set
			if (nodeName == null || nodeName.isEmpty()) {
				log.warn("parentServer.name is null for ComputeServer ${server.id} (${server.name}). Attempting to find node for VM ID ${vmId}.")
				HttpApiClient tempClient = new HttpApiClient()
				try {
					def vmsList = ProxmoxApiComputeUtil.listVMs(tempClient, authConfig)?.data
					def foundVm = vmsList?.find { it.vmid.toString() == vmId }
					if (foundVm?.node) {
						nodeName = foundVm.node
						log.info("Found node '${nodeName}' for VM ID ${vmId} by querying Proxmox API.")
					} else {
						return ServiceResponse.error("Missing nodeName (parentServer.name) for server ${server.name} and could not find it via API.")
					}
				} finally {
					tempClient?.shutdownClient()
				}
			}

			// Default firewallEnabled to false if null
			Boolean effectiveFirewallEnabled = firewallEnabled ?: false

			log.info("Attempting to add NIC to VM ${vmId} on node ${nodeName}: Bridge=${bridgeName}, Model=${model}, VLAN=${vlanTag}, Firewall=${effectiveFirewallEnabled}")
			ServiceResponse response = ProxmoxApiComputeUtil.addVMNetworkInterface(client, authConfig, nodeName, vmId, bridgeName.trim(), model.trim(), vlanTag, effectiveFirewallEnabled)

			if (response.success) {
				log.info("Successfully initiated add NIC for VM ${vmId} on node ${nodeName}. Response: ${response.msg}")
			} else {
				log.error("Failed to add NIC to VM ${vmId} on node ${nodeName}: ${response.msg}")
			}
			return response

		} catch (e) {
			log.error("Error in addNetworkInterfaceToServer for VM ${server?.externalId}: ${e.message}", e)
			return ServiceResponse.error("Error adding network interface to server ${server?.name}: ${e.message}")
		} finally {
			client?.shutdownClient()
		}
	}

	/**
	 * Gets the console connection details for the specified ComputeServer (VM).
	 *
	 * @param server The ComputeServer (VM) for which to get console details.
	 * @param opts Additional options map (may specify console type preference in the future).
	 * @return ServiceResponse containing a map with console details (e.g., url, type, ticket).
	 */
       ServiceResponse getVmConsoleDetails(ComputeServer server, Map opts) {
               log.info("ProxmoxVeCloudProvider.getVmConsoleDetails called for server: ${server?.id} (${server?.name}) with opts: ${opts}")
               HttpApiClient client = new HttpApiClient()
               try {
                       if(!server?.managed) {
                               return ServiceResponse.error("Console access only supported for managed VMs")
                       }
                       if(!server?.cloud) {
                               return ServiceResponse.error("ComputeServer cloud information is missing.")
                       }
                       if(!server.cloud.configMap?.enableConsoleAccess?.toString()?.toBoolean()) {
                               return ServiceResponse.error("Console access not enabled on cloud")
                       }

                       Map authConfig = plugin.getAuthConfig(server.cloud)
                       String vmId = server.externalId
                       if(!vmId)
                               return ServiceResponse.error("Missing externalId (VM ID) for server ${server.name}")

                       String nodeName = server.parentServer?.name
                       if(!nodeName) {
                               log.debug("Looking up node for VM ${vmId}")
                               def vmsList = ProxmoxApiComputeUtil.listVMs(client, authConfig)?.data
                               def foundVm = vmsList?.find { it.vmid.toString() == vmId }
                               if(foundVm?.node) {
                                       nodeName = foundVm.node
                               } else {
                                       return ServiceResponse.error("Unable to determine Proxmox node for VM ${vmId}")
                               }
                       }

                       ServiceResponse tokenResp = ProxmoxApiComputeUtil.getApiV2Token(authConfig)
                       if(!tokenResp.success)
                               return tokenResp
                       def tokenCfg = tokenResp.data

                       String path = "${authConfig.v2basePath}/nodes/${nodeName}/qemu/${vmId}/vncproxy"
                       def body = [websocket: 1]
                       def optsReq = new HttpApiClient.RequestOptions(
                               headers: [
                                       'Content-Type':'application/json',
                                       'Cookie': "PVEAuthCookie=${tokenCfg.token}",
                                       'CSRFPreventionToken': tokenCfg.csrfToken
                               ],
                               body: body,
                               contentType: ContentType.APPLICATION_JSON,
                               ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
                       )

                       log.debug("POST ${authConfig.apiUrl}${path} body ${body}")
                       ServiceResponse apiResp = ProxmoxApiUtil.callJsonApiWithRetry(client, authConfig.apiUrl, path, null, null, optsReq, 'POST')
                       if(!apiResp.success)
                               return ProxmoxApiUtil.validateApiResponse(apiResp, "Failed to start console for VM ${vmId}")

                       Map data = apiResp.data?.data
                       if(!data?.port || !data?.vncticket)
                               return ServiceResponse.error("Proxmox API did not return port or ticket")

                       URI baseUri = new URI(authConfig.apiUrl)
                       String host = baseUri.host
                       int uiPort = baseUri.port != -1 ? baseUri.port : 8006
                       String scheme = baseUri.scheme == 'https' ? 'wss' : 'ws'
                       String wsUrl = "${scheme}://${host}:${uiPort}${authConfig.v2basePath}/nodes/${nodeName}/qemu/${vmId}/vncwebsocket?port=${data.port}&vncticket=${URLEncoder.encode(data.vncticket.toString(), 'UTF-8')}"

                       ConsoleAccess access = new ConsoleAccess(consoleType: 'vnc', targetHost: host, port: data.port as Integer, ticket: data.vncticket.toString(), webSocketUrl: wsUrl)
                       log.debug("Console access prepared: ${access.toMap()}")
                       return ServiceResponse.success(access)

               } catch(e) {
                       log.error("Error in getVmConsoleDetails for VM ${server?.externalId}: ${e.message}", e)
                       return ServiceResponse.error("Error getting console details for server ${server?.name}: ${e.message}")
               } finally {
                       client?.shutdownClient()
               }
       }

	ServiceResponse listVMSnapshots(ComputeServer server) {
		log.info("ProxmoxVeCloudProvider.listVMSnapshots called for server: ${server?.id} (${server?.name})")
		HttpApiClient client = new HttpApiClient()
		try {
			if (server == null) {
				return ServiceResponse.error("ComputeServer cannot be null.")
			}
			if (server.cloud == null) {
				return ServiceResponse.error("ComputeServer cloud information is missing.")
			}

			Map authConfig = plugin.getAuthConfig(server.cloud)
			String vmId = server.externalId
			String nodeName = server.parentServer?.name

			if (vmId == null || vmId.isEmpty()) {
				return ServiceResponse.error("Missing externalId (VM ID) for server ${server.name}")
			}

			if (nodeName == null || nodeName.isEmpty()) {
				log.warn("parentServer.name is null for ComputeServer ${server.id} (${server.name}). Attempting to find node for VM ID ${vmId}.")
				HttpApiClient tempClient = new HttpApiClient()
				try {
					def vmsList = ProxmoxApiComputeUtil.listVMs(tempClient, authConfig)?.data
					def foundVm = vmsList?.find { it.vmid.toString() == vmId }
					if (foundVm?.node) {
						nodeName = foundVm.node
						log.info("Found node '${nodeName}' for VM ID ${vmId} by querying Proxmox API.")
					} else {
						return ServiceResponse.error("Missing nodeName (parentServer.name) for server ${server.name} and could not find it via API.")
					}
				} finally {
					tempClient?.shutdownClient()
				}
			}

			log.info("Attempting to list snapshots for VM ${vmId} on node ${nodeName}")
			ServiceResponse response = ProxmoxApiComputeUtil.listSnapshots(client, authConfig, nodeName, vmId)

			if (response.success) {
				log.info("Successfully listed snapshots for VM ${vmId} on node ${nodeName}.")
			} else {
				log.error("Failed to list snapshots for VM ${vmId} on node ${nodeName}: ${response.msg}")
			}
			return response

		} catch (e) {
			log.error("Error in listVMSnapshots for VM ${server?.externalId}: ${e.message}", e)
			return ServiceResponse.error("Error listing snapshots for server ${server?.name}: ${e.message}")
		} finally {
			client?.shutdownClient()
		}
	}

	ServiceResponse createVMSnapshot(ComputeServer server, String snapshotName, String description) {
		log.info("ProxmoxVeCloudProvider.createVMSnapshot called for server: ${server?.id} (${server?.name}), snapshotName: ${snapshotName}")
		HttpApiClient client = new HttpApiClient()
		try {
			if (server == null) {
				return ServiceResponse.error("ComputeServer cannot be null.")
			}
			if (server.cloud == null) {
				return ServiceResponse.error("ComputeServer cloud information is missing.")
			}
			if (snapshotName == null || snapshotName.trim().isEmpty()) {
				return ServiceResponse.error("Snapshot name cannot be empty.")
			}

			Map authConfig = plugin.getAuthConfig(server.cloud)
			String vmId = server.externalId
			String nodeName = server.parentServer?.name

			if (vmId == null || vmId.isEmpty()) {
				return ServiceResponse.error("Missing externalId (VM ID) for server ${server.name}")
			}

			if (nodeName == null || nodeName.isEmpty()) {
				log.warn("parentServer.name is null for ComputeServer ${server.id} (${server.name}). Attempting to find node for VM ID ${vmId}.")
				HttpApiClient tempClient = new HttpApiClient()
				try {
					def vmsList = ProxmoxApiComputeUtil.listVMs(tempClient, authConfig)?.data
					def foundVm = vmsList?.find { it.vmid.toString() == vmId }
					if (foundVm?.node) {
						nodeName = foundVm.node
						log.info("Found node '${nodeName}' for VM ID ${vmId} by querying Proxmox API.")
					} else {
						return ServiceResponse.error("Missing nodeName (parentServer.name) for server ${server.name} and could not find it via API.")
					}
				} finally {
					tempClient?.shutdownClient()
				}
			}
			String effectiveDescription = description ?: "" // Ensure description is not null

			log.info("Attempting to create snapshot '${snapshotName}' for VM ${vmId} on node ${nodeName}")
			ServiceResponse response = ProxmoxApiComputeUtil.createSnapshot(client, authConfig, nodeName, vmId, snapshotName, effectiveDescription)

			if (response.success) {
				log.info("Successfully initiated snapshot creation '${snapshotName}' for VM ${vmId} on node ${nodeName}. Task ID: ${response.data?.taskId}")
			} else {
				log.error("Failed to create snapshot '${snapshotName}' for VM ${vmId} on node ${nodeName}: ${response.msg}")
			}
			return response

		} catch (e) {
			log.error("Error in createVMSnapshot for VM ${server?.externalId}: ${e.message}", e)
			return ServiceResponse.error("Error creating snapshot for server ${server?.name}: ${e.message}")
		} finally {
			client?.shutdownClient()
		}
	}

	ServiceResponse deleteVMSnapshot(ComputeServer server, String snapshotName) {
		log.info("ProxmoxVeCloudProvider.deleteVMSnapshot called for server: ${server?.id} (${server?.name}), snapshotName: ${snapshotName}")
		HttpApiClient client = new HttpApiClient()
		try {
			if (server == null) {
				return ServiceResponse.error("ComputeServer cannot be null.")
			}
			if (server.cloud == null) {
				return ServiceResponse.error("ComputeServer cloud information is missing.")
			}
			if (snapshotName == null || snapshotName.trim().isEmpty()) {
				return ServiceResponse.error("Snapshot name cannot be empty.")
			}

			Map authConfig = plugin.getAuthConfig(server.cloud)
			String vmId = server.externalId
			String nodeName = server.parentServer?.name

			if (vmId == null || vmId.isEmpty()) {
				return ServiceResponse.error("Missing externalId (VM ID) for server ${server.name}")
			}

			if (nodeName == null || nodeName.isEmpty()) {
				log.warn("parentServer.name is null for ComputeServer ${server.id} (${server.name}). Attempting to find node for VM ID ${vmId}.")
				HttpApiClient tempClient = new HttpApiClient()
				try {
					def vmsList = ProxmoxApiComputeUtil.listVMs(tempClient, authConfig)?.data
					def foundVm = vmsList?.find { it.vmid.toString() == vmId }
					if (foundVm?.node) {
						nodeName = foundVm.node
						log.info("Found node '${nodeName}' for VM ID ${vmId} by querying Proxmox API.")
					} else {
						return ServiceResponse.error("Missing nodeName (parentServer.name) for server ${server.name} and could not find it via API.")
					}
				} finally {
					tempClient?.shutdownClient()
				}
			}

			log.info("Attempting to delete snapshot '${snapshotName}' for VM ${vmId} on node ${nodeName}")
			ServiceResponse response = ProxmoxApiComputeUtil.deleteSnapshot(client, authConfig, nodeName, vmId, snapshotName)

			if (response.success) {
				log.info("Successfully initiated snapshot deletion '${snapshotName}' for VM ${vmId} on node ${nodeName}. Task ID: ${response.data?.taskId}")
			} else {
				log.error("Failed to delete snapshot '${snapshotName}' for VM ${vmId} on node ${nodeName}: ${response.msg}")
			}
			return response

		} catch (e) {
			log.error("Error in deleteVMSnapshot for VM ${server?.externalId}: ${e.message}", e)
			return ServiceResponse.error("Error deleting snapshot for server ${server?.name}: ${e.message}")
		} finally {
			client?.shutdownClient()
		}
	}

	ServiceResponse revertToVMSnapshot(ComputeServer server, String snapshotName) {
		log.info("ProxmoxVeCloudProvider.revertToVMSnapshot called for server: ${server?.id} (${server?.name}), snapshotName: ${snapshotName}")
		HttpApiClient client = new HttpApiClient()
		try {
			if (server == null) {
				return ServiceResponse.error("ComputeServer cannot be null.")
			}
			if (server.cloud == null) {
				return ServiceResponse.error("ComputeServer cloud information is missing.")
			}
			if (snapshotName == null || snapshotName.trim().isEmpty()) {
				return ServiceResponse.error("Snapshot name cannot be empty.")
			}

			Map authConfig = plugin.getAuthConfig(server.cloud)
			String vmId = server.externalId
			String nodeName = server.parentServer?.name

			if (vmId == null || vmId.isEmpty()) {
				return ServiceResponse.error("Missing externalId (VM ID) for server ${server.name}")
			}

			if (nodeName == null || nodeName.isEmpty()) {
				log.warn("parentServer.name is null for ComputeServer ${server.id} (${server.name}). Attempting to find node for VM ID ${vmId}.")
				HttpApiClient tempClient = new HttpApiClient()
				try {
					def vmsList = ProxmoxApiComputeUtil.listVMs(tempClient, authConfig)?.data
					def foundVm = vmsList?.find { it.vmid.toString() == vmId }
					if (foundVm?.node) {
						nodeName = foundVm.node
						log.info("Found node '${nodeName}' for VM ID ${vmId} by querying Proxmox API.")
					} else {
						return ServiceResponse.error("Missing nodeName (parentServer.name) for server ${server.name} and could not find it via API.")
					}
				} finally {
					tempClient?.shutdownClient()
				}
			}

			log.info("Attempting to revert to snapshot '${snapshotName}' for VM ${vmId} on node ${nodeName}")
			ServiceResponse response = ProxmoxApiComputeUtil.rollbackSnapshot(client, authConfig, nodeName, vmId, snapshotName)

			if (response.success) {
				log.info("Successfully initiated revert to snapshot '${snapshotName}' for VM ${vmId} on node ${nodeName}. Task ID: ${response.data?.taskId}")
			} else {
				log.error("Failed to revert to snapshot '${snapshotName}' for VM ${vmId} on node ${nodeName}: ${response.msg}")
			}
			return response

		} catch (e) {
			log.error("Error in revertToVMSnapshot for VM ${server?.externalId}: ${e.message}", e)
			return ServiceResponse.error("Error reverting to snapshot for server ${server?.name}: ${e.message}")
		} finally {
			client?.shutdownClient()
		}
	}

	/**
	 * Removes a disk from the specified ComputeServer (VM).
	 *
	 * @param server The ComputeServer (VM) to remove the disk from.
	 * @param diskName The name of the disk to remove (e.g., 'scsi1', 'virtio0').
	 * @return ServiceResponse indicating success or failure, and potentially a taskId from Proxmox.
	 */
	ServiceResponse removeDiskFromServer(ComputeServer server, String diskName) {
		log.info("ProxmoxVeCloudProvider.removeDiskFromServer called for server: ${server?.id} (${server?.name}), diskName: ${diskName}")
		HttpApiClient client = new HttpApiClient()
		try {
			// Basic input validation
			if (server == null) {
				return ServiceResponse.error("ComputeServer cannot be null.")
			}
			if (server.cloud == null) {
				return ServiceResponse.error("ComputeServer cloud information is missing.")
			}
			if (diskName == null || diskName.trim().isEmpty()) {
				return ServiceResponse.error("Disk name cannot be empty.")
			}

			Map authConfig = plugin.getAuthConfig(server.cloud)
			String vmId = server.externalId
			String nodeName = server.parentServer?.name

			if (vmId == null || vmId.isEmpty()) {
				return ServiceResponse.error("Missing externalId (VM ID) for server ${server.name}")
			}

			// Fallback for nodeName if parentServer is not set or nodeName is not directly available
			if (nodeName == null || nodeName.isEmpty()) {
				log.warn("parentServer.name is null or empty for ComputeServer ${server.id} (${server.name}). Attempting to find node for VM ID ${vmId}.")
				// Use a temporary client for this internal lookup.
				// Note: The main 'client' will be used for the actual removeVMDisk operation.
				HttpApiClient tempClient = new HttpApiClient()
				try {
					// It's important that listVMs doesn't require the client to be pre-authenticated with a specific token,
					// as getApiV2Token is called within listVMs and ProxmoxApiComputeUtil methods.
					def vmsListResponse = ProxmoxApiComputeUtil.listVMs(tempClient, authConfig)
					if (!vmsListResponse.success) {
						log.error("Failed to retrieve VM list to find nodeName for VM ${vmId}: ${vmsListResponse.msg}")
						return ServiceResponse.error("Could not determine node for VM ${vmId}: Failed to list VMs. ${vmsListResponse.msg}")
					}
					def foundVm = vmsListResponse.data?.find { it.vmid.toString() == vmId }
					if (foundVm?.node) {
						nodeName = foundVm.node
						log.info("Found node '${nodeName}' for VM ID ${vmId} by querying Proxmox API.")
					} else {
						log.error("Could not find node for VM ID ${vmId} via Proxmox API listVMs.")
						return ServiceResponse.error("Missing nodeName (parentServer.name) for server ${server.name} and could not find it via API.")
					}
				} finally {
					tempClient?.shutdownClient()
				}
			}

			log.info("Attempting to remove disk '${diskName}' from VM ${vmId} on node ${nodeName}")
			ServiceResponse response = ProxmoxApiComputeUtil.removeVMDisk(client, authConfig, nodeName, vmId, diskName)

			if (response.success) {
				log.info("Successfully initiated remove disk '${diskName}' for VM ${vmId} on node ${nodeName}. Task ID: ${response.data?.taskId}")
			} else {
				log.error("Failed to remove disk '${diskName}' for VM ${vmId} on node ${nodeName}: ${response.msg}")
			}
			return response

		} catch (e) {
			log.error("Error in removeDiskFromServer for VM ${server?.externalId}, disk ${diskName}: ${e.message}", e)
			return ServiceResponse.error("Error removing disk ${diskName} from server ${server?.name}: ${e.message}")
		} finally {
			client?.shutdownClient()
		}
	}

	/**
	 * Removes a network interface from the specified ComputeServer (VM).
	 *
	 * @param server The ComputeServer (VM) to remove the network interface from.
	 * @param interfaceName The name of the network interface to remove (e.g., 'net0', 'net1').
	 * @return ServiceResponse indicating success or failure, and potentially a taskId from Proxmox.
	 */
	ServiceResponse removeNetworkInterfaceFromServer(ComputeServer server, String interfaceName) {
		log.info("ProxmoxVeCloudProvider.removeNetworkInterfaceFromServer called for server: ${server?.id} (${server?.name}), interfaceName: ${interfaceName}")
		HttpApiClient client = new HttpApiClient()
		try {
			// Basic input validation
			if (server == null) {
				return ServiceResponse.error("ComputeServer cannot be null.")
			}
			if (server.cloud == null) {
				return ServiceResponse.error("ComputeServer cloud information is missing.")
			}
			if (interfaceName == null || interfaceName.trim().isEmpty()) {
				return ServiceResponse.error("Network interface name (e.g., net0) cannot be empty.")
			}

			Map authConfig = plugin.getAuthConfig(server.cloud)
			String vmId = server.externalId
			String nodeName = server.parentServer?.name

			if (vmId == null || vmId.isEmpty()) {
				return ServiceResponse.error("Missing externalId (VM ID) for server ${server.name}")
			}

			// Fallback for nodeName
			if (nodeName == null || nodeName.isEmpty()) {
				log.warn("parentServer.name is null or empty for ComputeServer ${server.id} (${server.name}). Attempting to find node for VM ID ${vmId}.")
				HttpApiClient tempClient = new HttpApiClient()
				try {
					def vmsListResponse = ProxmoxApiComputeUtil.listVMs(tempClient, authConfig)
					if (!vmsListResponse.success) {
						log.error("Failed to retrieve VM list to find nodeName for VM ${vmId}: ${vmsListResponse.msg}")
						return ServiceResponse.error("Could not determine node for VM ${vmId}: Failed to list VMs. ${vmsListResponse.msg}")
					}
					def foundVm = vmsListResponse.data?.find { it.vmid.toString() == vmId }
					if (foundVm?.node) {
						nodeName = foundVm.node
						log.info("Found node '${nodeName}' for VM ID ${vmId} by querying Proxmox API.")
					} else {
						log.error("Could not find node for VM ID ${vmId} via Proxmox API listVMs.")
						return ServiceResponse.error("Missing nodeName for server ${server.name} and could not find it via API.")
					}
				} finally {
					tempClient?.shutdownClient()
				}
			}

			log.info("Attempting to remove network interface '${interfaceName}' from VM ${vmId} on node ${nodeName}")
			ServiceResponse response = ProxmoxApiComputeUtil.removeVMNetworkInterface(client, authConfig, nodeName, vmId, interfaceName)

			if (response.success) {
				log.info("Successfully initiated removal of network interface '${interfaceName}' for VM ${vmId}. Task ID: ${response.data?.taskId}")
			} else {
				log.error("Failed to remove network interface '${interfaceName}' for VM ${vmId}: ${response.msg}")
			}
			return response

		} catch (e) {
			log.error("Error in removeNetworkInterfaceFromServer for VM ${server?.externalId}, interface ${interfaceName}: ${e.message}", e)
			return ServiceResponse.error("Error removing network interface ${interfaceName} from server ${server?.name}: ${e.message}")
		} finally {
			client?.shutdownClient()
		}
	}

	/**
	 * Updates an existing network interface on the specified ComputeServer (VM).
	 *
	 * @param server The ComputeServer (VM) to update the network interface on.
	 * @param interfaceName The name of the network interface to update (e.g., 'net0').
	 * @param bridgeName The new bridge name for the interface (e.g., 'vmbr1').
	 * @param model The new model for the interface (e.g., 'virtio').
	 * @param vlanTag (Optional) The new VLAN ID. Null or empty means no VLAN tag or keep existing if Proxmox defaults.
	 * @param firewallEnabled (Optional) Boolean to enable/disable Proxmox firewall. Null means no change to firewall setting.
	 * @return ServiceResponse indicating success or failure, and potentially a taskId from Proxmox.
	 */
        ServiceResponse updateNetworkInterfaceOnServer(ComputeServer server, String interfaceName, String bridgeName, String model, String vlanTag, Boolean firewallEnabled) {
                log.info("ProxmoxVeCloudProvider.updateNetworkInterfaceOnServer called for server: ${server?.id} (${server?.name}), interface: ${interfaceName}, bridge: ${bridgeName}, model: ${model}, vlan: ${vlanTag}, firewall: ${firewallEnabled}")
                HttpApiClient client = new HttpApiClient()
                try {
			// Basic input validation
			if (server == null) {
				return ServiceResponse.error("ComputeServer cannot be null.")
			}
			if (server.cloud == null) {
				return ServiceResponse.error("ComputeServer cloud information is missing.")
			}
			if (interfaceName == null || interfaceName.trim().isEmpty()) {
				return ServiceResponse.error("Network interface name (e.g., net0) cannot be empty.")
			}
			if (bridgeName == null || bridgeName.trim().isEmpty()) {
				return ServiceResponse.error("Bridge name cannot be empty.")
			}
			if (model == null || model.trim().isEmpty()) {
				return ServiceResponse.error("Network card model cannot be empty.")
			}
			// Known models validation (optional, Proxmox will validate anyway)
			List<String> knownModels = ['e1000', 'e1000-82545em', 'virtio', 'rtl8139', 'vmxnet3', 'i82551', 'i82557b', 'i82559er']
			if (!knownModels.contains(model.toLowerCase())) {
				log.warn("Potentially unknown network model specified: ${model}. Proxmox will validate.")
			}

			Map authConfig = plugin.getAuthConfig(server.cloud)
			String vmId = server.externalId
			String nodeName = server.parentServer?.name

			if (vmId == null || vmId.isEmpty()) {
				return ServiceResponse.error("Missing externalId (VM ID) for server ${server.name}")
			}

			// Fallback for nodeName
			if (nodeName == null || nodeName.isEmpty()) {
				log.warn("parentServer.name is null or empty for ComputeServer ${server.id} (${server.name}). Attempting to find node for VM ID ${vmId}.")
				HttpApiClient tempClient = new HttpApiClient()
				try {
					def vmsListResponse = ProxmoxApiComputeUtil.listVMs(tempClient, authConfig)
					if (!vmsListResponse.success) {
						log.error("Failed to retrieve VM list to find nodeName for VM ${vmId}: ${vmsListResponse.msg}")
						return ServiceResponse.error("Could not determine node for VM ${vmId}: Failed to list VMs. ${vmsListResponse.msg}")
					}
					def foundVm = vmsListResponse.data?.find { it.vmid.toString() == vmId }
					if (foundVm?.node) {
						nodeName = foundVm.node
						log.info("Found node '${nodeName}' for VM ID ${vmId} by querying Proxmox API.")
					} else {
						log.error("Could not find node for VM ID ${vmId} via Proxmox API listVMs.")
						return ServiceResponse.error("Missing nodeName for server ${server.name} and could not find it via API.")
					}
				} finally {
					tempClient?.shutdownClient()
				}
			}

			log.info("Attempting to update network interface '${interfaceName}' on VM ${vmId} on node ${nodeName}")
			ServiceResponse response = ProxmoxApiComputeUtil.updateVMNetworkInterface(client, authConfig, nodeName, vmId, interfaceName, bridgeName.trim(), model.trim(), vlanTag, firewallEnabled)

			if (response.success) {
				log.info("Successfully initiated update of network interface '${interfaceName}' for VM ${vmId}. Task ID: ${response.data?.taskId}")
			} else {
				log.error("Failed to update network interface '${interfaceName}' for VM ${vmId}: ${response.msg}")
			}
			return response

		} catch (e) {
			log.error("Error in updateNetworkInterfaceOnServer for VM ${server?.externalId}, interface ${interfaceName}: ${e.message}", e)
			return ServiceResponse.error("Error updating network interface ${interfaceName} on server ${server?.name}: ${e.message}")
		} finally {
                        client?.shutdownClient()
                }
        }

        /**
         * Migrate a VM to another node.
         * @param server The VM to migrate
         * @param destinationNode target node name
         * @param live true for live migration
         * @return ServiceResponse from the Proxmox API
         */
        ServiceResponse migrateServer(ComputeServer server, String destinationNode, Boolean live) {
                log.info("ProxmoxVeCloudProvider.migrateServer called for server: ${server?.id} (${server?.name}), destination: ${destinationNode}, live: ${live}")
                HttpApiClient client = new HttpApiClient()
                try {
                        if(server == null)
                                return ServiceResponse.error("ComputeServer cannot be null.")
                        if(server.cloud == null)
                                return ServiceResponse.error("ComputeServer cloud information is missing.")
                        if(!destinationNode)
                                return ServiceResponse.error("Destination node cannot be empty.")

                        Map authConfig = plugin.getAuthConfig(server.cloud)
                        String vmId = server.externalId
                        String nodeName = server.parentServer?.name
                        if(!vmId)
                                return ServiceResponse.error("Missing externalId (VM ID) for server ${server.name}")

                        if(!nodeName) {
                                log.warn("parentServer.name is null for ComputeServer ${server.id} (${server.name}). Attempting to find node for VM ID ${vmId}.")
                                HttpApiClient tempClient = new HttpApiClient()
                                try {
                                        def vmsListResponse = ProxmoxApiComputeUtil.listVMs(tempClient, authConfig)
                                        if(!vmsListResponse.success)
                                                return ServiceResponse.error("Could not determine node for VM ${vmId}: ${vmsListResponse.msg}")
                                        def foundVm = vmsListResponse.data?.find { it.vmid.toString() == vmId }
                                        if(foundVm?.node) {
                                                nodeName = foundVm.node
                                                log.info("Found node '${nodeName}' for VM ID ${vmId} by querying Proxmox API.")
                                        } else {
                                                return ServiceResponse.error("Missing nodeName for server ${server.name} and could not find it via API.")
                                        }
                                } finally {
                                        tempClient?.shutdownClient()
                                }
                        }

                        ServiceResponse response = ProxmoxApiComputeUtil.migrateVm(client, authConfig, nodeName, vmId, destinationNode, live ?: false)

                        if(response.success) {
                                log.info("Successfully initiated migration of VM ${vmId} to node ${destinationNode}")
                                // Update parent server reference
                                def destProj = context.async.computeServer.listIdentityProjections(server.cloud.id, null).filter {
                                        ComputeServerIdentityProjection p -> p.category == "proxmox.ve.host.${server.cloud.id}" && (p.name == destinationNode || p.externalId == destinationNode)
                                }.blockingFirst(null)
                                if(destProj) {
                                        server.parentServer = context.async.computeServer.get(destProj.id).blockingGet()
                                        context.async.computeServer.bulkSave([server]).blockingGet()
                                } else {
                                        log.warn("Could not find ComputeServer record for destination node ${destinationNode} to update parent reference")
                                }
                        } else {
                                log.error("Failed to migrate VM ${vmId} to node ${destinationNode}: ${response.msg}")
                        }
                        return response
                } catch(e) {
                        log.error("Error migrating server ${server?.externalId} to node ${destinationNode}: ${e.message}", e)
                        return ServiceResponse.error("Error migrating server ${server?.name}: ${e.message}")
                } finally {
                        client?.shutdownClient()
                }
        }

        ServiceResponse rebootServer(ComputeServer computeServer) {
                HttpApiClient client = new HttpApiClient()
                try {
                        Map authConfig = plugin.getAuthConfig(computeServer.cloud)
                        if(computeServer.serverType == 'hypervisor') {
                                String nodeName = computeServer.externalId
                                return ProxmoxApiComputeUtil.rebootNode(client, authConfig, nodeName)
                        }
                        String vmId = computeServer.externalId
                        String nodeName = computeServer.parentServer?.name
                        if (!vmId)
                                return ServiceResponse.error("Missing externalId (VM ID) for server ${computeServer.name}")
                        if (!nodeName) {
                                def vmsList = ProxmoxApiComputeUtil.listVMs(client, authConfig)?.data
                                def foundVm = vmsList?.find { it.vmid == vmId }
                                if(foundVm?.node)
                                        nodeName = foundVm.node
                                else
                                        return ServiceResponse.error("Missing nodeName (parentServer.name) for server ${computeServer.name} and could not find it via API.")
                        }
                        return ProxmoxApiComputeUtil.rebootVM(client, authConfig, nodeName, vmId)
                } catch(e) {
                        log.error("Error performing reboot on server ${computeServer.externalId}: ${e.message}", e)
                        return ServiceResponse.error("Error performing reboot on server ${computeServer.externalId}: ${e.message}")
                } finally {
                        client?.shutdownClient()
                }
        }
}
