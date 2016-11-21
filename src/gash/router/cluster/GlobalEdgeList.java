/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.cluster;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * GlobalEdgeList contains the list of edges
 *
 */
public class GlobalEdgeList {
	protected ConcurrentHashMap<String, GlobalEdgeInfo> map = new ConcurrentHashMap<String, GlobalEdgeInfo>();

	public GlobalEdgeList() {
	}

	public GlobalEdgeInfo createIfNew(int ref, String host, int port) {
		if (hasNode(host))
			return getNode(host);
		else
			return addNode(ref, host, port);
	}

	public GlobalEdgeInfo addNode(int ref, String host, int port) {
		if (!verify(ref, host, port)) {
			// TODO log error
			throw new RuntimeException("Invalid node info");
		}

		if (!hasNode(host)) {
			GlobalEdgeInfo ei = new GlobalEdgeInfo(ref, host, port);
			map.put(host, ei);
			return ei;
		} else
			return null;
	}

	private boolean verify(int ref, String host, int port) {
		if (ref < 0 || host == null || port < 1024)
			return false;
		else
			return true;
	}

	public boolean hasNode(String host) {
		return map.containsKey(host);

	}
	
	public ConcurrentHashMap<String, GlobalEdgeInfo> getEdgeListMap(){
		return map;
	}
	
	public GlobalEdgeInfo getNode(String host) {
		return map.get(host);
	}

	public void removeNode(String host) {
		map.remove(host);
	}

	public void clear() {
		map.clear();
	}
	
	public boolean isEmpty() {
		if (map.size() > 0) {
			return false;
		}
		return true;
	}
}