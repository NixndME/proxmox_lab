package com.morpheus.proxmox.ve

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.providers.NetworkProvider
import com.morpheusdata.core.providers.CloudInitializationProvider
import com.morpheusdata.core.providers.SecurityGroupProvider
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.CloudPool
import com.morpheusdata.model.Network
import com.morpheusdata.model.NetworkRoute
import com.morpheusdata.model.NetworkRouter
import com.morpheusdata.model.NetworkRouterType
import com.morpheusdata.model.NetworkServer
import com.morpheusdata.model.NetworkServerType
import com.morpheusdata.model.NetworkSubnet
import com.morpheusdata.model.NetworkType
import com.morpheusdata.model.OptionType
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.core.util.MorpheusUtils
import groovy.util.logging.Slf4j

@Slf4j
class ProxmoxNetworkProvider implements NetworkProvider, CloudInitializationProvider {

	ProxmoxVePlugin plugin
	MorpheusContext morpheus
	SecurityGroupProvider securityGroupProvider

	final String code = 'proxmox'
	final String name = 'Proxmox'
	final String description = 'Proxmox'

	ProxmoxNetworkProvider(ProxmoxVePlugin plugin, MorpheusContext morpheusContext) {
		this.plugin = plugin
		this.morpheus = morpheusContext
	}

	@Override
	String getNetworkServerTypeCode() {
		return 'proxmox-ve.network'
	}

	/**
	 * The CloudProvider code that this NetworkProvider should be attached to.
	 * When this NetworkProvider is registered with Morpheus, all Clouds that match this code will have a
	 * NetworkServer of this type attached to them. Network actions will then be handled via this provider.
	 * @return String Code of the Cloud type
	 */
	@Override
	String getCloudProviderCode() {
		return 'proxmox-ve.cloud'
	}

	/**
	 * Provides a Collection of NetworkTypes that can be managed by this provider
	 * @return Collection of NetworkType
	 */
	@Override
	Collection<NetworkType> getNetworkTypes() {
		NetworkType bridgeNetwork = new NetworkType([
				code              : 'proxmox-ve-bridge-network',
				externalType      : 'LinuxBridge',
				cidrEditable      : false,
				dhcpServerEditable: false,
				dnsEditable       : true,
				gatewayEditable   : false,
				vlanIdEditable    : false,
				canAssignPool     : true,
				name              : 'Proxmox VE Bridge Network',
				hasNetworkServer  : true,
				creatable: true
		])

		return [bridgeNetwork]

	}


	@Override
	Collection<OptionType> getOptionTypes() {
		return null
	}

	@Override
	Collection<OptionType> getSecurityGroupOptionTypes() {
		return []
	}

	@Override
	ServiceResponse initializeProvider(Cloud cloud) {
		log.info("Initializeing network provider for ${cloud.name}")
		ServiceResponse rtn = ServiceResponse.prepare()
		try {
			NetworkServer networkServer = new NetworkServer(
				name: cloud.name,
				type: new NetworkServerType(code:"proxmox-ve.network")
			)
			morpheus.integration.registerCloudIntegration(cloud.id, networkServer).blockingGet()
			rtn.success = true
		} catch (Exception e) {
			rtn.success = false
			log.error("initializeProvider error: {}", e, e)
		}

		return rtn
	}

	@Override
	ServiceResponse deleteProvider(Cloud cloud) {
		log.info("Deleting network provider for ${cloud.name}")
		ServiceResponse rtn = ServiceResponse.prepare()
		try {
			// cleanup is done by type, so we do not need to load the record
			// NetworkServer networkServer = morpheusContext.services.networkServer.find(new DataQuery().withFilters([new DataFilter('type.code', "amazon"), new DataFilter('zoneId', cloud.id)]))
			// NetworkServer networkServer = cloud.networkServer // this works too, ha
			NetworkServer networkServer = new NetworkServer(
				name: cloud.name,
				type: new NetworkServerType(code:"proxmox-ve.network")
			)
			morpheus.integration.deleteCloudIntegration(cloud.id, networkServer).blockingGet()
			rtn.success = true
		} catch (Exception e) {
			rtn.success = false
			log.error("deleteProvider error: {}", e, e)
		}

		return rtn
	}

	/**
	 * Creates the Network submitted
	 * @param network Network information
	 * @param opts additional configuration options
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse createNetwork(Network network, Map opts) {
		return ServiceResponse.success(network)
	}

	/**
	 * Updates the Network submitted
	 * @param network Network information
	 * @param opts additional configuration options
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse<Network> updateNetwork(Network network, Map opts) {
		return ServiceResponse.success(network)
	}

	/**
	 * Deletes the Network submitted
	 * @param network Network information
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse deleteNetwork(Network network, Map opts) {
		return ServiceResponse.success()
	}
	
	@Override
	ServiceResponse createSubnet(NetworkSubnet subnet, Network network, Map opts) {
		return ServiceResponse.success()	
	}
	
	@Override
	ServiceResponse updateSubnet(NetworkSubnet subnet, Network network, Map opts) {
		return ServiceResponse.success()	
	}
	
	@Override
	ServiceResponse deleteSubnet(NetworkSubnet subnet, Network network, Map opts) {
		return ServiceResponse.success()
	}
	
	@Override
	Collection getRouterTypes() {
		return []
	}

}
