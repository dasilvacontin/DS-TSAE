/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package lsim.element.recipes_service;

import java.io.IOException;
import java.io.ObjectInputStream;
//import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Random;

import edu.uoc.dpcs.lsim.LSimFactory;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import edu.uoc.dpcs.lsim.utils.LSimParameters;
import lsim.application.handler.Handler;
import recipes_service.ServerData;
import recipes_service.ServerPartnerSide;
import recipes_service.activity_simulation.SimulationData;
import recipes_service.communication.Host;
import util.Serializer;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class WorkerInitHandler implements Handler {
	
	private ServerData serverData;
	private Host localNode;
	private String instanceDescription=null;
//	private String testServerAddress = "sd.uoc.edu";
//    private int port = 54324;
//    private boolean defaultTestServer = true;
	@Override
	public Object execute(Object obj) {
 		LSimParameters params = (LSimParameters) obj;
		
		System.out.println("XIVATO1:" + params.toString());
		System.out.println("XIVATO1:" + ((LSimParameters)params.get("coordinatorLSimParameters")));
		System.out.println("XIVATO1:" + ((LSimParameters)params.get("coordinatorLSimParameters")).get("serverBasePort"));
	
		instanceDescription = (String)params.get("instanceDescription");
		System.out.println("XIVATO1: instanceId" + instanceDescription);
		
		// param 0: base port
		int port = Integer.valueOf(((String)((LSimParameters)params.get("coordinatorLSimParameters")).get("serverBasePort")));
		
		// param 1: groupId
		String groupId = ((String)((LSimParameters)params.get("coordinatorLSimParameters")).get("groupId"));

		// new serverData 
		serverData = new ServerData(groupId);

		// params 2 and 3: TSAE parameters 
		serverData.setSessionDelay(Long.parseLong((String)((LSimParameters)params.get("coordinatorLSimParameters")).get("sessionDelay"))*1000);
		serverData.setSessionPeriod(Long.parseLong((String)((LSimParameters)params.get("coordinatorLSimParameters")).get("sessionPeriod"))*1000);
		
		serverData.setNumberSessions(Integer.parseInt((String)((LSimParameters)params.get("coordinatorLSimParameters")).get("numSes"))*1000);
		serverData.setPropagationDegree(Integer.parseInt((String)((LSimParameters)params.get("coordinatorLSimParameters")).get("propDegree"))*1000);

		// params 4 to 11: simulation parameters
		SimulationData.getInstance().setSimulationStop(Integer.parseInt((String)((LSimParameters)params.get("coordinatorLSimParameters")).get("simulationStop"))*1000);
		SimulationData.getInstance().setExecutionStop(Integer.parseInt((String)((LSimParameters)params.get("coordinatorLSimParameters")).get("executionStop"))*1000);

		Random rnd = new Random();
		int simulationDelay = (int) (rnd.nextDouble() * (2 * Integer.parseInt((String)((LSimParameters)params.get("coordinatorLSimParameters")).get("simulationDelay")) * 1000));
		SimulationData.getInstance().setSimulationDelay(simulationDelay);
		SimulationData.getInstance().setSimulationPeriod(Integer.parseInt((String)((LSimParameters)params.get("coordinatorLSimParameters")).get("simulationPeriod"))*1000);

		SimulationData.getInstance().setProbDisconnect(Double.parseDouble((String)((LSimParameters)params.get("coordinatorLSimParameters")).get("probDisconnect")));
		SimulationData.getInstance().setProbReconnect(Double.parseDouble((String)((LSimParameters)params.get("coordinatorLSimParameters")).get("probReconnect")));
		SimulationData.getInstance().setProbCreate(Double.parseDouble((String)((LSimParameters)params.get("coordinatorLSimParameters")).get("probCreate")));
		SimulationData.getInstance().setProbDel(Double.parseDouble((String)((LSimParameters)params.get("coordinatorLSimParameters")).get("probDel")));

		SimulationData.getInstance().setDeletion(!(Double.parseDouble((String)((LSimParameters)params.get("coordinatorLSimParameters")).get("probDel")) == 0.0));

		SimulationData.getInstance().setSamplingTime(Integer.parseInt((String)((LSimParameters)params.get("coordinatorLSimParameters")).get("samplingTime"))*1000);
		
		// param 12: "purge": purges log; "no purge": deactivates the purge of log
		// default value: purge. (Any value different from !"no purge" will result in purge mode)
		SimulationData.getInstance().setPurge(!((String)((LSimParameters)params.get("coordinatorLSimParameters")).get("purge")).equals("nopurge"));

		// param 13: to indicate if all Servers will run in a single computer
		// or they will run Servers hosted in different computers (or more than one 
		// Server in a single computer but this computer having the same internal and external IP address)
		// * true: all Server run in a single computer
		// * false: Servers running in different computers (or more than one Server in a single computer but
		// 			this computer having the same internal and external IP address)
		SimulationData.getInstance().setLocalExecution(((String)((LSimParameters)params.get("coordinatorLSimParameters")).get("executionMode")).equals("localMode"));
		
		
		//         this computer having the same internal and external IP address) 
		// publish the service in the first empty port staring on obj.get(0)
		// (starts a thread to deal with TSAE sessions from partner servers)
		// set connected state on simulation data
		ServerPartnerSide serverPartnerSide = new ServerPartnerSide(port, serverData);
		serverPartnerSide.start();
		
		String hostAddress = null;
		
		if (SimulationData.getInstance().localExecution()){
			try {
				hostAddress = InetAddress.getLocalHost().getHostAddress();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		} else {
			hostAddress = getHostAddress();
		}

		// waits until the serverPartnerSide has published the service in a port
		serverPartnerSide.waitServicePublished();
		
		String id = null;
		// create id
		id = groupId+"@"+hostAddress+":"+serverPartnerSide.getPort();

		// set id on serverData
		serverData.setId(id);
			
		// createlocal node information to send to coordinator node
		localNode = new Host(hostAddress, serverPartnerSide.getPort(), id);
		
        // init return value
		Object returnObj = null;
		try {
			returnObj = Serializer.serialize(localNode);
		} catch (IOException e) {
			// TODO Auto-generated catch block		List<Object> params = (List<Object>) obj;

			e.printStackTrace();
		}

		return returnObj;
	}

	public Host getLocalNode(){
		return localNode;
	}
	
	public ServerData getServerData(){
		return serverData; 
	}
	
	/*
	 * Auxiliary methods
	 */
	private String getHostAddress(){
		Socket socket = null;
        ObjectInputStream in = null;
        //String testServerAddress = "sd.uoc.edu";
        String testServerAddress = "213.73.35.47";
        int port = 54324;
        String hostAddress = null;
        try {
        	socket = new Socket(testServerAddress, port);
        	in = new ObjectInputStream(socket.getInputStream());
        	hostAddress = (String) in.readObject();
        	in.close();
        	socket.close();
        } catch (IOException e) {
//        	System.err.println("WorkerInitiHandler -- getHostAddress -- Couldn't get I/O for "
//        			+ "the connection to: " + testServerAddress);
        	LSimFactory.getWorkerInstance().log(
    				Level.ERROR,
    				"WorkerInitiHandler -- getHostAddress -- Couldn't get I/O for "
    	        			+ "the connection to: " + testServerAddress
        			);
        	System.exit(1);
        } catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
        	LSimFactory.getWorkerInstance().log(
    				Level.ERROR,
    				e.getMessage()
        			);
  			e.printStackTrace();
		}
        return hostAddress;
	}
	
	public String getInstanceDescription(){
		return instanceDescription;
	}
//	public void setTestServerAddress(String testServerAddress, int port){
//		this.testServerAddress = testServerAddress;
//        this.port = port;
//        this.defaultTestServer = false;
//	}
}