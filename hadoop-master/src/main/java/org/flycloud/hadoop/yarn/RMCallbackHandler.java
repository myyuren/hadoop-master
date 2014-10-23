package org.flycloud.hadoop.yarn;

import java.util.List;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
	/**
	 * 
	 */
	private final ApplicationMaster myApplicationMaster;

	/**
	 * @param myApplicationMaster
	 */
	RMCallbackHandler(ApplicationMaster myApplicationMaster) {
		this.myApplicationMaster = myApplicationMaster;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onContainersCompleted(List<ContainerStatus> completedContainers) {
		ApplicationMaster.LOG
				.info("Got response from RM for container ask, completedCnt="
						+ completedContainers.size());
		for (ContainerStatus containerStatus : completedContainers) {
			ApplicationMaster.LOG.info("Got container status for containerID="
					+ containerStatus.getContainerId() + ", state="
					+ containerStatus.getState() + ", exitStatus="
					+ containerStatus.getExitStatus() + ", diagnostics="
					+ containerStatus.getDiagnostics());

			// non complete containers should not be here
			assert (containerStatus.getState() == ContainerState.COMPLETE);

			// increment counters for completed/failed containers
			int exitStatus = containerStatus.getExitStatus();
			if (0 != exitStatus) {
				// container failed
				if (ContainerExitStatus.ABORTED != exitStatus) {
					// shell script failed
					// counts as completed
					this.myApplicationMaster.numCompletedContainers
							.incrementAndGet();
					this.myApplicationMaster.numFailedContainers
							.incrementAndGet();
				} else {
					// container was killed by framework, possibly preempted
					// we should re-try as the container was lost for some
					// reason
					this.myApplicationMaster.numAllocatedContainers
							.decrementAndGet();
					this.myApplicationMaster.numRequestedContainers
							.decrementAndGet();
					// we do not need to release the container as it would be
					// done
					// by the RM
				}
			} else {
				// nothing to do
				// container completed successfully
				this.myApplicationMaster.numCompletedContainers
						.incrementAndGet();
				ApplicationMaster.LOG.info("Container completed successfully."
						+ ", containerId=" + containerStatus.getContainerId());
			}
		}

		// ask for more containers if any failed
		int askCount = this.myApplicationMaster.numTotalContainers
				- this.myApplicationMaster.numRequestedContainers.get();
		this.myApplicationMaster.numRequestedContainers.addAndGet(askCount);

		if (askCount > 0) {
			for (int i = 0; i < askCount; ++i) {
				ContainerRequest containerAsk = this.myApplicationMaster
						.setupContainerAskForRM();
				this.myApplicationMaster.amRMClient
						.addContainerRequest(containerAsk);
			}
		}

		if (this.myApplicationMaster.numCompletedContainers.get() == this.myApplicationMaster.numTotalContainers) {
			this.myApplicationMaster.done = true;
		}
	}

	@Override
	public void onContainersAllocated(List<Container> allocatedContainers) {
		ApplicationMaster.LOG
				.info("Got response from RM for container ask, allocatedCnt="
						+ allocatedContainers.size());
		this.myApplicationMaster.numAllocatedContainers
				.addAndGet(allocatedContainers.size());
		for (Container allocatedContainer : allocatedContainers) {
			ApplicationMaster.LOG
					.info("Launching shell command on a new container."
							+ ", containerId=" + allocatedContainer.getId()
							+ ", containerNode="
							+ allocatedContainer.getNodeId().getHost() + ":"
							+ allocatedContainer.getNodeId().getPort()
							+ ", containerNodeURI="
							+ allocatedContainer.getNodeHttpAddress()
							+ ", containerResourceMemory"
							+ allocatedContainer.getResource().getMemory());
			// + ", containerToken"
			// +allocatedContainer.getContainerToken().getIdentifier().toString());

			LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(
					this.myApplicationMaster, allocatedContainer,
					this.myApplicationMaster.containerListener);
			Thread launchThread = new Thread(runnableLaunchContainer);

			// launch and start the container on a separate thread to keep
			// the main thread unblocked
			// as all containers may not be allocated at one go.
			this.myApplicationMaster.launchThreads.add(launchThread);
			launchThread.start();
		}
	}

	@Override
	public void onShutdownRequest() {
		this.myApplicationMaster.done = true;
	}

	@Override
	public void onNodesUpdated(List<NodeReport> updatedNodes) {
	}

	@Override
	public float getProgress() {
		// set progress to deliver to RM on next heartbeat
		float progress = (float) this.myApplicationMaster.numCompletedContainers
				.get() / this.myApplicationMaster.numTotalContainers;
		return progress;
	}

	@Override
	public void onError(Throwable e) {
		this.myApplicationMaster.done = true;
		this.myApplicationMaster.amRMClient.stop();
	}
}