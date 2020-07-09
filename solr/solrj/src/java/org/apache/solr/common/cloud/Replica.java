/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.common.cloud;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.Utils;

import static org.apache.solr.common.ConditionalMapWriter.NON_NULL_VAL;
import static org.apache.solr.common.ConditionalMapWriter.dedupeKeyPredicate;

public class Replica extends ZkNodeProps implements MapWriter {
  
  /**
   * The replica's state. In general, if the node the replica is hosted on is
   * not under {@code /live_nodes} in ZK, the replica's state should be
   * discarded.
   */
  public enum State {
    
    /**
     * The replica is ready to receive updates and queries.
     * <p>
     * <b>NOTE</b>: when the node the replica is hosted on crashes, the
     * replica's state may remain ACTIVE in ZK. To determine if the replica is
     * truly active, you must also verify that its {@link Replica#getNode()
     * node} is under {@code /live_nodes} in ZK (or use
     * {@link ClusterState#liveNodesContain(String)}).
     * </p>
     */
    ACTIVE,
    
    /**
     * The first state before {@link State#RECOVERING}. A node in this state
     * should be actively trying to move to {@link State#RECOVERING}.
     * <p>
     * <b>NOTE</b>: a replica's state may appear DOWN in ZK also when the node
     * it's hosted on gracefully shuts down. This is a best effort though, and
     * should not be relied on.
     * </p>
     */
    DOWN,
    
    /**
     * The node is recovering from the leader. This might involve peer-sync,
     * full replication or finding out things are already in sync.
     */
    RECOVERING,
    
    /**
     * Recovery attempts have not worked, something is not right.
     * <p>
     * <b>NOTE</b>: This state doesn't matter if the node is not part of
     * {@code /live_nodes} in ZK; in that case the node is not part of the
     * cluster and it's state should be discarded.
     * </p>
     */
    RECOVERY_FAILED;
    
    @Override
    public String toString() {
      return super.toString().toLowerCase(Locale.ROOT);
    }
    
    /** Converts the state string to a State instance. */
    public static State getState(String stateStr) {
      return stateStr == null ? null : State.valueOf(stateStr.toUpperCase(Locale.ROOT));
    }
  }

  public enum Type {
    /**
     * Writes updates to transaction log and indexes locally. Replicas of type {@link Type#NRT} support NRT (soft commits) and RTG. 
     * Any {@link Type#NRT} replica can become a leader. A shard leader will forward updates to all active {@link Type#NRT} and
     * {@link Type#TLOG} replicas. 
     */
    NRT,
    /**
     * Writes to transaction log, but not to index, uses replication. Any {@link Type#TLOG} replica can become leader (by first
     * applying all local transaction log elements). If a replica is of type {@link Type#TLOG} but is also the leader, it will behave 
     * as a {@link Type#NRT}. A shard leader will forward updates to all active {@link Type#NRT} and {@link Type#TLOG} replicas.
     */
    TLOG,
    /**
     * Doesn’t index or writes to transaction log. Just replicates from {@link Type#NRT} or {@link Type#TLOG} replicas. {@link Type#PULL}
     * replicas can’t become shard leaders (i.e., if there are only pull replicas in the collection at some point, updates will fail
     * same as if there is no leaders, queries continue to work), so they don’t even participate in elections.
     */
    PULL;

    public static Type get(String name){
      return name == null ? Type.NRT : Type.valueOf(name.toUpperCase(Locale.ROOT));
    }
  }

  // coreNode name
  public final String name;
  public final String node;
  public final String core;
  public final State state;
  public final Type type;
  public final String shard, collection;
  public final boolean isLeader;

  public Replica(String name, Map<String,Object> map, String collection, String shard) {
    super(new HashMap<>());
    this.collection = collection;
    map.remove(ZkStateReader.COLLECTION_PROP);
    this.shard = shard;
    map.remove(ZkStateReader.SHARD_ID_PROP);
    this.name = name;
    map.remove(ZkStateReader.CORE_NODE_NAME_PROP);
    this.node = (String) map.get(ZkStateReader.NODE_NAME_PROP);
    map.remove(ZkStateReader.NODE_NAME_PROP);
    this.core = (String) map.get(ZkStateReader.CORE_NAME_PROP);
    map.remove(ZkStateReader.CORE_NAME_PROP);
    type = Type.get((String) map.remove(ZkStateReader.REPLICA_TYPE));
    if (propMap.get(ZkStateReader.STATE_PROP) != null) {
      this.state = State.getState((String) map.remove(ZkStateReader.STATE_PROP));
    } else {
      this.state = State.ACTIVE;                         //Default to ACTIVE
    }
    this.isLeader = Boolean.parseBoolean(String.valueOf(map.getOrDefault(ZkStateReader.LEADER_PROP, "false")));
    this.propMap.putAll(map);
    validate();
  }

  // clone constructor
  public Replica(String name, String node, String collection, String shard, String core,
                  boolean isLeader, State state, Type type, Map<String, Object> props) {
    super(new HashMap<>());
    this.name = name;
    this.node = node;
    this.state = state;
    this.type = type;
    this.isLeader = isLeader;
    this.collection = collection;
    this.shard = shard;
    this.core = core;
    if (props != null) {
      this.propMap.putAll(props);
    }
  }

  /**
   * This constructor uses a map with one key (coreNode name) and a value that
   * is a map containing all replica properties.
   * @param nestedMap nested map containing replica properties
   */
  public Replica(Map<String, Object> nestedMap) {
    this.name = nestedMap.keySet().iterator().next();
    Map<String, Object> details = (Map<String, Object>) nestedMap.get(name);
    Objects.requireNonNull(details);
    details = Utils.getDeepCopy(details, 4);
    this.collection = (String) details.get("collection");
    this.shard = (String) details.get("shard");
    this.core = (String) details.get("core");
    this.node = (String) details.get("node_name");
    this.isLeader = Boolean.parseBoolean(String.valueOf(details.getOrDefault(ZkStateReader.LEADER_PROP, "false")));
    type = Replica.Type.valueOf(String.valueOf(details.getOrDefault(ZkStateReader.REPLICA_TYPE, "NRT")));
    state = State.getState(String.valueOf(details.getOrDefault(ZkStateReader.STATE_PROP, "active")));
    this.propMap.putAll(details);
    validate();

  }

  private final void validate() {
    Objects.requireNonNull(this.name, "'name' must not be null");
    Objects.requireNonNull(this.core, "'core' must not be null");
    Objects.requireNonNull(this.collection, "'collection' must not be null");
    Objects.requireNonNull(this.shard, "'shard' must not be null");
    Objects.requireNonNull(this.type, "'type' must not be null");
    Objects.requireNonNull(this.state, "'state' must not be null");
    Objects.requireNonNull(this.node, "'node' must not be null");
  }

  public String getCollection() {
    return collection;
  }

  public String getShard() {
    return shard;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    Replica other = (Replica) o;

    if (
        name.equals(other.name) &&
            collection.equals(other.collection) &&
            core.equals(other.core) &&
            isLeader == other.isLeader &&
            node.equals(other.node) &&
            shard.equals(other.shard) &&
            type == other.type &&
            propMap.equals(other.propMap)) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, core, collection, shard, type, state, propMap);
  }

  /** Also known as coreNodeName. */
  public String getName() {
    return name;
  }

  public String getCoreUrl() {
    return ZkCoreNodeProps.getCoreUrl(getStr(ZkStateReader.BASE_URL_PROP), core);
  }

  public String getBaseUrl() {
    return getStr(ZkStateReader.BASE_URL_PROP);
  }

  /** SolrCore name. */
  public String getCoreName() {
    return core;
  }

  /** The name of the node this replica resides on */
  public String getNode() {
    return node;
  }
  
  /** Returns the {@link State} of this replica. */
  public State getState() {
    return state;
  }

  public boolean isActive(Set<String> liveNodes) {
    return this.node != null && liveNodes.contains(this.node) && this.state == State.ACTIVE;
  }
  
  public Type getType() {
    return this.type;
  }

  public String getProperty(String propertyName) {
    final String propertyKey;
    if (!propertyName.startsWith(ZkStateReader.PROPERTY_PROP_PREFIX)) {
      propertyKey = ZkStateReader.PROPERTY_PROP_PREFIX + propertyName;
    } else {
      propertyKey = propertyName;
    }
    final String propertyValue = getStr(propertyKey);
    return propertyValue;
  }

  public Object get(String propName) {
    return propMap.get(propName);
  }

  public Object get(String propName, Object defValue) {
    Object o = propMap.get(propName);
    if (o != null) {
      return o;
    } else {
      return defValue;
    }
  }

  public Object clone() {
    return new Replica(name, node, collection, shard, core, isLeader, state, type,
        propMap);
  }

  @Override
  public void writeMap(MapWriter.EntryWriter ew) throws IOException {
    BiPredicate<CharSequence, Object> p = dedupeKeyPredicate(new HashSet<>())
        .and(NON_NULL_VAL);
    ew.put(name, (MapWriter) ew1 -> {
      ew1.put(ZkStateReader.CORE_NAME_PROP, core, p)
          .put(ZkStateReader.SHARD_ID_PROP, shard, p)
          .put(ZkStateReader.COLLECTION_PROP, collection, p)
          .put(ZkStateReader.NODE_NAME_PROP, node, p)
          .put(ZkStateReader.REPLICA_TYPE, type.toString(), p)
          .put(ZkStateReader.STATE_PROP, state.toString(), p)
          .put(ZkStateReader.LEADER_PROP, isLeader, p);
      for (Map.Entry<String, Object> e : propMap.entrySet()) ew1.put(e.getKey(), e.getValue(), p);
    });
  }

  @Override
  public String toString() {
    return Utils.toJSONString(this); // small enough, keep it on one line (i.e. no indent)
  }
}
