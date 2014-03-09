package es.uam.eps.ir.ebro;

import es.uam.eps.ir.ebro.Ebro.Vertex;
import gnu.trove.impl.Constants;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntByteMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntByteHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TByteProcedure;
import gnu.trove.procedure.TIntProcedure;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Ebro<V extends Vertex> {

    private static final byte NOHALT = (byte) 0;
    private static final byte HALT = (byte) 1;
    private int superstep;
    private final TIntObjectMap<V> vertices;
    private TIntObjectMap<List> currentMessages;
    private TIntObjectMap<List> futureMessages;
    private final TIntByteMap votes;
    private final ExecutorService threadPool;
    private int n;

    public Ebro(int nthreads, int nvertices) {
        this.superstep = 0;
        this.vertices = new TIntObjectHashMap<>(nvertices, Constants.DEFAULT_LOAD_FACTOR, -1);
        this.currentMessages = new TIntObjectHashMap<>(nvertices, Constants.DEFAULT_LOAD_FACTOR, -1);
        this.futureMessages = new TIntObjectHashMap<>(nvertices, Constants.DEFAULT_LOAD_FACTOR, -1);
        this.votes = new TIntByteHashMap(nvertices, Constants.DEFAULT_LOAD_FACTOR, -1, HALT);

        if (nthreads > 0) {
            this.threadPool = Executors.newFixedThreadPool(nthreads, new CustomThreadFactory());
        } else {
            this.threadPool = null;
        }
        
        n = 0;
    }

    public int addVertex(V v) {
        v.id = n;
        v.ebro = this;
        vertices.put(v.id, v);
        currentMessages.put(v.id, Collections.synchronizedList(new ArrayList()));
        futureMessages.put(v.id, Collections.synchronizedList(new ArrayList()));
        votes.put(v.id, NOHALT);

        n++;
        
        return v.id;
    }

    public V getVertex(int id) {
        return vertices.get(id);
    }

    public void addEdge(int from, int to, double w) {
        vertices.get(from).addEdge(to, w);
    }

    public void run() {
        while (!stop()) {
            System.out.println(superstep());
            doSuperstep();
        }
    }

    private boolean hasMessages() {
        for (List m : currentMessages.valueCollection()) {
            if (!m.isEmpty()) {
                return true;
            }
        }

        return false;
    }

    private boolean allToHalt() {
        return votes.forEachValue(new TByteProcedure() {

            @Override
            public boolean execute(byte value) {
                return value == HALT;
            }
        });
    }

    public boolean stop() {
        return allToHalt() && !hasMessages();
    }

    public void doSuperstep() {

        final List<Callable<Object>> tasks = new ArrayList<>();
        vertices.forEachKey(new TIntProcedure() {

            @Override
            public boolean execute(final int id) {
                tasks.add(Executors.callable(new Runnable() {

                    @Override
                    public void run() {
                        List messages = currentMessages.get(id);
                        if (votes.get(id) == NOHALT || !messages.isEmpty()) {
                            votes.put(id, NOHALT);
                            vertices.get(id).compute(messages);
                        }
                        messages.clear();
                    }
                }));

                return true;
            }
        });

        if (threadPool != null) {
            try {
                threadPool.invokeAll(tasks);

            } catch (InterruptedException ex) {
                Logger.getLogger(Ebro.class
                        .getName()).log(Level.SEVERE, null, ex);
            }
        } else {
            for (Callable<Object> task : tasks) {
                try {
                    task.call();

                } catch (Exception ex) {
                    Logger.getLogger(Ebro.class
                            .getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

        tasks.clear();

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
        protected Ebro<Vertex<M>> ebro;
        protected final TIntList edgeDestList;
        protected final TDoubleList edgeWeightList;

        public Vertex() {
            this.edgeDestList = new TIntArrayList();
            this.edgeWeightList = new TDoubleArrayList();
        }

        protected abstract void compute(Iterable<M> messages);

        protected final void addEdge(int dst, double weight) {
            edgeDestList.add(dst);
            edgeWeightList.add(weight);
        }

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
}
