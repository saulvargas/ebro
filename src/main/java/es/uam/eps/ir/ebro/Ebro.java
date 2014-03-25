package es.uam.eps.ir.ebro;

import es.uam.eps.ir.ebro.Ebro.Vertex;
import gnu.trove.impl.Constants;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntByteMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntByteHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Ebro {

    private static final byte NOHALT = (byte) 0;
    private static final byte HALT = (byte) 1;
    private int superstep;
    private final Map<Integer, Vertex> vertices;
    private final TIntObjectMap<Aggregator> aggregators;
    private TIntObjectMap<List> currentMessages;
    private TIntObjectMap<List> futureMessages;
    private final TIntByteMap votes;
    private final ExecutorService threadPool;
    private final int nthreads;
    private int vertexCount;
    private int aggregatorCount;
    private final boolean directed;
    private final boolean weighted;

    public Ebro(int nthreads, int nvertices, boolean directed, boolean weighted) {
        this.superstep = 0;
        this.vertices = new HashMap<>();
        this.aggregators = new TIntObjectHashMap<>();
        this.currentMessages = new TIntObjectHashMap<>(nvertices, Constants.DEFAULT_LOAD_FACTOR, -1);
        this.futureMessages = new TIntObjectHashMap<>(nvertices, Constants.DEFAULT_LOAD_FACTOR, -1);
        this.votes = new TIntByteHashMap(nvertices, Constants.DEFAULT_LOAD_FACTOR, -1, HALT);
        this.directed = directed;
        this.weighted = weighted;

        this.nthreads = nthreads;
        if (nthreads > 0) {
            this.threadPool = Executors.newFixedThreadPool(nthreads, new CustomThreadFactory());
        } else {
            this.threadPool = null;
        }

        vertexCount = 0;
        aggregatorCount = 0;
    }

    public int addVertex(Vertex v) {
        v.configure(vertexCount, this, weighted);
        vertices.put(v.id, v);
        currentMessages.put(v.id, Collections.synchronizedList(new ArrayList()));
        futureMessages.put(v.id, Collections.synchronizedList(new ArrayList()));
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

    public Vertex getVertex(int id) {
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
        return currentMessages.valueCollection().stream().mapToInt(List::size).sum();
    }

    private boolean hasMessages() {
        return !currentMessages.valueCollection().stream().allMatch(List::isEmpty);
    }

    private boolean allToHalt() {
        return votes.forEachValue(value -> value == HALT);
    }

    public boolean stop() {
        return allToHalt() && !hasMessages();
    }

    public void doSuperstep() {

        final List<Callable<Object>> tasks = new ArrayList<>();
        vertices.forEach((id, vertex) -> {
            tasks.add(Executors.callable(() -> {
                List messages = currentMessages.get(id);
                if (votes.get(id) == NOHALT || !messages.isEmpty()) {
                    votes.put(id, NOHALT);
                    vertex.compute(messages);
                }
                messages.clear();
            }));
        });

        if (nthreads < 0) {
            vertices.entrySet().parallelStream().forEach(e -> {
                List messages = currentMessages.get(e.getKey());
                if (votes.get(e.getKey()) == NOHALT || !messages.isEmpty()) {
                    votes.put(e.getKey(), NOHALT);
                    e.getValue().compute(messages);
                }
                messages.clear();
            });
        } else {
            if (threadPool != null) {
                try {
                    threadPool.invokeAll(tasks);

                } catch (InterruptedException ex) {
                    Logger.getLogger(Ebro.class
                            .getName()).log(Level.SEVERE, null, ex);
                }
            } else {
                tasks.stream().forEach(task -> {
                    try {
                        task.call();
                    } catch (Exception ex) {
                        Logger.getLogger(Ebro.class.getName()).log(Level.SEVERE, null, ex);
                    }
                });
            }

            tasks.clear();
        }

        aggregators.valueCollection().stream().forEach(Aggregator::nextStep);

        superstep++;
        TIntObjectMap<List> aux;
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

    protected void sendMessage(int dst, Object message) {
        futureMessages.get(dst).add(message);

    }

    private static class CustomThreadFactory implements ThreadFactory {

        private static final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

        @Override
        public Thread newThread(Runnable r) {
            Thread t = defaultFactory.newThread(r);
            t.setDaemon(true);
            return t;
        }
    };

    public static abstract class Vertex<M> {

        protected int id;
        protected Ebro ebro;
        protected final TIntArrayList edgeDestList;
        protected TDoubleArrayList edgeWeightList;

        public Vertex() {
            this.edgeDestList = new TIntArrayList();
        }

        private void configure(int id, Ebro ebro, boolean weighted) {
            this.id = id;
            this.ebro = ebro;
            if (weighted) {
                edgeWeightList = new TDoubleArrayList();
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
