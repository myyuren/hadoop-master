package org.flycloud.hadoop.yarn;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

class LaunchContainerRunnable implements Runnable {

	private final ApplicationMaster myApplicationMaster;

	private Container container;

	private NMCallbackHandler containerListener;

	public LaunchContainerRunnable(ApplicationMaster myApplicationMaster,
			Container lcontainer, NMCallbackHandler containerListener) {
		this.myApplicationMaster = myApplicationMaster;
		this.container = lcontainer;
		this.containerListener = containerListener;
	}

	private List<String> getCommands() {
		Vector<CharSequence> vargs = new Vector<CharSequence>(30);
		vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
		vargs.add("-Xmx" + myApplicationMaster.containerMemory + "m");
		vargs.add(myApplicationMaster.containerClass);

		vargs.add("1>" + "/tmp/task-stdout.log");
		vargs.add("2>" + "/tmp/task-stderr.log");

		StringBuilder command = new StringBuilder();
		for (CharSequence str : vargs) {
			command.append(str).append(" ");
		}

		List<String> commands = new ArrayList<String>();
		commands.add(command.toString());
		return commands;
	}

	private Map<String, String> getEnv() {
		Map<String, String> env = new HashMap<String, String>();
		StringBuilder classPathEnv = new StringBuilder(
				Environment.CLASSPATH.$()).append(File.pathSeparatorChar)
				.append("./*");
		Configuration conf = new Configuration();
		for (String c : conf.getStrings(
				YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			classPathEnv.append(File.pathSeparatorChar);
			classPathEnv.append(c.trim());
		}
		classPathEnv.append(File.pathSeparatorChar)
				.append("./log4j.properties");
		classPathEnv.append(File.pathSeparatorChar);
		classPathEnv.append(System.getProperty("java.class.path"));
		classPathEnv.append(File.pathSeparatorChar).append("./Node.jar");
		env.put("CLASSPATH", classPathEnv.toString());
		return env;
	}

	@Override
	public void run() {
		ApplicationMaster.LOG
				.info("Setting up container launch container for containerid="
						+ container.getId());
		ContainerLaunchContext ctx = Records
				.newRecord(ContainerLaunchContext.class);
		ctx.setEnvironment(getEnv());
		ctx.setLocalResources(getLocalResources());
		ctx.setCommands(getCommands());
		ctx.setTokens(this.myApplicationMaster.allTokens.duplicate());

		containerListener.addContainer(container.getId(), container);
		this.myApplicationMaster.nmClientAsync.startContainerAsync(container,
				ctx);
	}

	private Map<String, LocalResource> getLocalResources() {
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path dst = new Path(fs.getHomeDirectory(), "Node.jar");
			FileStatus shellFileStatus = fs.getFileStatus(dst);
			LocalResource shellRsrc = Records.newRecord(LocalResource.class);
			shellRsrc.setType(LocalResourceType.FILE);
			shellRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
			shellRsrc.setResource(ConverterUtils.getYarnUrlFromPath(dst));
			shellRsrc.setTimestamp(shellFileStatus.getModificationTime());
			shellRsrc.setSize(shellFileStatus.getLen());
			localResources.put("Node.jar", shellRsrc);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return localResources;
	}

}