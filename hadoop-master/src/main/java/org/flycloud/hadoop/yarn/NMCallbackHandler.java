package org.flycloud.hadoop.yarn;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import com.google.common.annotations.VisibleForTesting;

@VisibleForTesting
class NMCallbackHandler implements NMClientAsync.CallbackHandler {

	private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();
	private final ApplicationMaster applicationMaster;

	public NMCallbackHandler(ApplicationMaster applicationMaster) {
		this.applicationMaster = applicationMaster;
	}

	public void addContainer(ContainerId containerId, Container container) {
		containers.putIfAbsent(containerId, container);
	}

	@Override
	public void onContainerStopped(ContainerId containerId) {
		if (ApplicationMaster.LOG.isDebugEnabled()) {
			ApplicationMaster.LOG.debug("Succeeded to stop Container "
					+ containerId);
		}
		containers.remove(containerId);
	}

	@Override
	public void onContainerStatusReceived(ContainerId containerId,
			ContainerStatus containerStatus) {
		if (ApplicationMaster.LOG.isDebugEnabled()) {
			ApplicationMaster.LOG.debug("Container Status: id=" + containerId
					+ ", status=" + containerStatus);
		}
	}

	@Override
	public void onContainerStarted(ContainerId containerId,
			Map<String, ByteBuffer> allServiceResponse) {
		if (ApplicationMaster.LOG.isDebugEnabled()) {
			ApplicationMaster.LOG.debug("Succeeded to start Container "
					+ containerId);
		}
		Container container = containers.get(containerId);
		if (container != null) {
			applicationMaster.nmClientAsync.getContainerStatusAsync(
					containerId, container.getNodeId());
		}
	}

	@Override
	public void onStartContainerError(ContainerId containerId, Throwable t) {
		ApplicationMaster.LOG.error("Failed to start Container " + containerId);
		containers.remove(containerId);
		applicationMaster.numCompletedContainers.incrementAndGet();
		applicationMaster.numFailedContainers.incrementAndGet();
	}

	@Override
	public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
		ApplicationMaster.LOG.error("Failed to query the status of Container "
				+ containerId);
	}

	@Override
	public void onStopContainerError(ContainerId containerId, Throwable t) {
		ApplicationMaster.LOG.error("Failed to stop Container " + containerId);
		containers.remove(containerId);
	}
}