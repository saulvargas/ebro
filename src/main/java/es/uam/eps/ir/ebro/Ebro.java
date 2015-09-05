/* 
 * Copyright (C) 2015 Saúl Vargas (saul@vargassandoval.es)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package es.uam.eps.ir.ebro;

import es.uam.eps.ir.ebro.Ebro.Vertex;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.Int2ByteMap;
import it.unimi.dsi.fastutil.ints.Int2ByteOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * The Pregel-like engine of the system.
 *
 * @author Saúl Vargas (saul@vargassandoval.es)
 * @param <M>
 */
public class Ebro<M> {

    private static final byte NOHALT = (byte) 0;
    private static final byte HALT = (byte) 1;
    private int superstep;
    private final Int2ObjectMap<Vertex<M>> vertices;
    private final Int2ObjectMap<Aggregator> aggregators;
    private Int2ObjectMap<List<M>> currentMessages;
    private Int2ObjectMap<List<M>> futureMessages;
    private final Int2ByteMap votes;
    private int vertexCount;
    private int aggregatorCount;
    private final boolean directed;
    private final boolean weighted;

    public Ebro(int nvertices, boolean directed, boolean weighted) {
        this.superstep = 0;
        this.vertices = new Int2ObjectOpenHashMap<>();
        this.aggregators = new Int2ObjectOpenHashMap<>();
        this.currentMessages = new Int2ObjectOpenHashMap<>(nvertices);
        this.futureMessages = new Int2ObjectOpenHashMap<>(nvertices);
        this.votes = new Int2ByteOpenHashMap(nvertices);
        this.votes.defaultReturnValue(HALT);
        this.directed = directed;
        this.weighted = weighted;

        vertexCount = 0;
        aggregatorCount = 0;
    }

    public int addVertex(Vertex<M> v) {
        v.configure(vertexCount, this, weighted);
        vertices.put(v.id, v);
        currentMessages.put(v.id, Collections.synchronizedList(new ArrayList<>()));
        futureMessages.put(v.id, Collections.synchronizedList(new ArrayList<>()));
        votes.put(v.id, NOHALT);

        vertexCount++;

        return v.id;
    }

    public int addAgregator(Aggregator a) {
        a.configure(aggregatorCount, this);
        aggregators.put(aggregatorCount, a);

        aggregatorCount++;

        return a.id;
    }

    public Vertex<M> getVertex(int id) {
        return vertices.get(id);
    }

    public void addEdge(int from, int to) {
        addEdge(from, to, 1.0);
    }

    public void addEdge(int from, int to, double w) {
        vertices.get(from).addEdge(to, w);
        if (!directed) {
            vertices.get(to).addEdge(from, w);
        }
    }

    public void run() {
        while (!stop()) {
            System.out.println(superstep() + "\t" + numMessages());
            doSuperstep();
        }
    }

    private int numMessages() {
        return currentMessages.values().stream().mapToInt(List::size).sum();
    }

    private boolean hasMessages() {
        return !currentMessages.values().stream().allMatch(List::isEmpty);
    }

    private boolean allToHalt() {
        return !votes.containsValue(NOHALT);
    }

    public boolean stop() {
        return allToHalt() && !hasMessages();
    }

    public void doSuperstep() {
        vertices.int2ObjectEntrySet().parallelStream().forEach(e -> {
            int id = e.getIntKey();
            Vertex<M> vertex = e.getValue();
            List<M> messages = currentMessages.get(e.getKey());
            if (votes.get(id) == NOHALT || !messages.isEmpty()) {
                votes.put(id, NOHALT);
                vertex.compute(messages);
            }
            messages.clear();
        });

        aggregators.values().stream().forEach(Aggregator::nextStep);

        superstep++;
        Int2ObjectMap<List<M>> aux;
        aux = currentMessages;
        currentMessages = futureMessages;
        futureMessages = aux;
    }

    public int superstep() {
        return superstep;
    }

    protected void voteToHalt(int id) {
        votes.put(id, HALT);
    }

    protected void sendMessage(int dst, M message) {
        futureMessages.get(dst).add(message);
    }

    public static abstract class Vertex<M> {

        protected int id;
        protected Ebro<M> ebro;
        protected final IntArrayList edgeDestList;
        protected DoubleArrayList edgeWeightList;

        public Vertex() {
            this.edgeDestList = new IntArrayList();
        }

        private void configure(int id, Ebro<M> ebro, boolean weighted) {
            this.id = id;
            this.ebro = ebro;
            if (weighted) {
                edgeWeightList = new DoubleArrayList();
            } else {
                edgeWeightList = null;
            }
        }

        private void addEdge(int dst, double weight) {
            edgeDestList.add(dst);
            if (edgeWeightList != null) {
                edgeWeightList.add(weight);
            }
        }

        protected abstract void compute(Collection<M> messages);

        protected final int superstep() {
            return ebro.superstep();
        }

        protected final void voteToHalt() {
            ebro.voteToHalt(id);
        }

        protected final void sendMessage(int dst, M message) {
            ebro.sendMessage(dst, message);
        }

    }

    public static abstract class Aggregator<T> {

        protected int id;
        protected Ebro ebro;
        protected T t0;
        protected T t1;

        public Aggregator() {
        }

        private void configure(int id, Ebro ebro) {
            this.id = id;
            this.ebro = ebro;
            this.t1 = init();
        }

        private void nextStep() {
            t0 = t1;
            t1 = init();
        }

        protected abstract T init();

        protected abstract T aggregate(T t1, T t);

        public synchronized void aggregate(T t) {
            t1 = aggregate(t1, t);
        }

        public T getValue() {
            return t0;
        }
    }
}
