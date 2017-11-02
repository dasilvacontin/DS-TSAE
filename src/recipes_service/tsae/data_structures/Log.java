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

package recipes_service.tsae.data_structures;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import edu.uoc.dpcs.lsim.LSimFactory;
import lsim.worker.LSimWorker;
import recipes_service.data.Operation;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class Log implements Serializable{
	// Needed for the logging system sgeag@2017
	@SuppressWarnings("unused")
	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

	private static final long serialVersionUID = -4864990265268259700L;
	/**
	 * This class implements a log, that stores the operations
	 * received  by a client.
	 * They are stored in a ConcurrentHashMap (a hash table),
	 * that stores a list of operations for each member of 
	 * the group.
	 */
	private ConcurrentHashMap<String, List<Operation>> log= new ConcurrentHashMap<String, List<Operation>>();  

	public Log(List<String> participants){
		// create an empty log
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			log.put(it.next(), new Vector<Operation>());
		}
	}

	/**
	 * inserts an operation into the log. Operations are 
	 * inserted in order. If the last operation for 
	 * the user is not the previous operation than the one 
	 * being inserted, the insertion will fail.
	 * 
	 * @param op
	 * @return true if op is inserted, false otherwise.
	 */
	public boolean add(Operation op){
		String hostid = op.getTimestamp().getHostid();
		List<Operation> operations = log.get(hostid);

		ListIterator<Operation> it = operations.listIterator();
		Operation lastOp = null;
		while (it.hasNext()) {
			lastOp = it.next();
		}
		if (lastOp != null) {
			Timestamp lastTs = lastOp.getTimestamp();
			Timestamp newTs = op.getTimestamp();
			if (lastTs.compare(newTs) > 0) {
				return false;
			}
		}
		
		it.add(op);
		return true;
	}
	
	/**
	 * Checks the received summary (sum) and determines the operations
	 * contained in the log that have not been seen by
	 * the proprietary of the summary.
	 * Returns them in an ordered list.
	 * @param sum
	 * @return list of operations
	 */
	public List<Operation> listNewer(TimestampVector sum){
		LinkedList<Operation> newerOperations = new LinkedList<Operation>();
		ListIterator<Operation> newerOpsIt= newerOperations.listIterator();
		log.forEach((BiConsumer<String, List<Operation>>) (node, nodeOps) -> {
			Timestamp nodeSumTs = sum.getLast(node);
			nodeOps.forEach((Consumer<Operation>) (nodeOp) -> {
				if (nodeOp.getTimestamp().compare(nodeSumTs) > 0) {
					newerOpsIt.add(nodeOp);
				}
			});
		});
		return newerOperations;
	}
	
	/**
	 * Removes from the log the operations that have
	 * been acknowledged by all the members
	 * of the group, according to the provided
	 * ackSummary. 
	 * @param ack: ackSummary.
	 */
	public void purgeLog(TimestampMatrix ack){
	}

	/**
	 * equals
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Log other = (Log) obj;

		Set<String> thisNodes = log.keySet();
		Set<String> otherNodes = other.log.keySet();
		if (!thisNodes.equals(otherNodes))
			return false;

		// same keys, compare operations for each node now
		// can't do equals directly on the map because it would compare
		// the List references instead of their content
		for (String node : log.keySet()) {
			List<Operation> thisOps = log.get(node);
			ListIterator<Operation> thisOpsIt = thisOps.listIterator();
			List<Operation> otherOps = other.log.get(node);
			ListIterator<Operation> otherOpsIt = otherOps.listIterator();

			while (thisOpsIt.hasNext() || otherOpsIt.hasNext()) {
				Operation thisOp = thisOpsIt.next();
				Operation otherOp = otherOpsIt.next();
				if (thisOp == null) {
					// means this list of ops finished before the other one
					return false;
				}
				if (!thisOp.equals(otherOp)) {
					return false;
				}
			}
		}
		
		return true;
	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String name="";
		for(Enumeration<List<Operation>> en=log.elements();
		en.hasMoreElements(); ){
		List<Operation> sublog=en.nextElement();
		for(ListIterator<Operation> en2=sublog.listIterator(); en2.hasNext();){
			name+=en2.next().toString()+"\n";
		}
	}
		
		return name;
	}
}