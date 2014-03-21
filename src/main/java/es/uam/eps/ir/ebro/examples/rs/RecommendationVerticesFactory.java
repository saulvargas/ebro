/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uam.eps.ir.ebro.examples.rs;

import es.uam.eps.ir.ebro.Ebro;
import es.uam.eps.ir.ebro.Ebro.Aggregator;
import es.uam.eps.ir.utils.dstructs.TIntDoubleTopN;
import gnu.trove.impl.Constants;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author saul
 */
public abstract class RecommendationVerticesFactory<U, I, M> {

    private final int cutoff;
    private final BufferedWriter writer;
    private final Ebro ebro;
    private final TObjectIntMap<U> users = new TObjectIntHashMap<>(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1);
    private final TObjectIntMap<I> items = new TObjectIntHashMap<>(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1);

    public RecommendationVerticesFactory(Ebro ebro, int cutoff, BufferedWriter writer) {
        this.ebro = ebro;
        this.cutoff = cutoff;
        this.writer = writer;
    }

    public void addUser(U u) {
        if (!users.containsKey(u)) {
            users.put(u, ebro.addVertex(createUserVertex(u)));
        }
    }

    public void addItem(I i) {
        if (!items.containsKey(i)) {
            items.put(i, ebro.addVertex(createItemVertex(i)));
        }
    }
    
    protected void addAggregator(Aggregator a) {
        ebro.addAgregator(a);
    }

    public void addEdge(U u, I i) {
        ebro.addEdge(users.get(u), items.get(i));
    }

    public Set<U> getUsers() {
        return users.keySet();
    }

    public Set<I> getItems() {
        return items.keySet();
    }

    public UserVertex<U, M> getUserVertex(U u) {
        return (UserVertex<U, M>) ebro.getVertex(users.get(u));
    }

    public ItemVertex<I, M> getItemVertex(I i) {
        return (ItemVertex<I, M>) ebro.getVertex(items.get(i));
    }

    public abstract UserVertex<U, M> createUserVertex(U u);

    public abstract ItemVertex<I, M> createItemVertex(I i);

    public abstract class ItemVertex<I, M> extends Ebro.Vertex<M> {

        public final I i_ml;

        public ItemVertex(I i_ml) {
            this.i_ml = i_ml;
        }

    }

    public abstract class UserVertex<U, M> extends Ebro.Vertex<M> {

        public final U u_ml;
        protected boolean active;

        public UserVertex(U u_ml) {
            this.u_ml = u_ml;
            this.active = false;
        }

        public void activate() {
            active = true;
        }
        
        public void sendMessageToAllItems(M message) {
            for (int i_id : items.values()) {
                sendMessage(i_id, message);
            }
        }

        protected void printResults(TIntDoubleMap scoresMap) {
            for (int i = 0; i < edgeDestList.size(); i++) {
                int i_id = edgeDestList.getQuick(i);
                scoresMap.remove(i_id);
            }

            final TIntDoubleTopN topN = new TIntDoubleTopN(cutoff);
            scoresMap.forEachEntry((a, b) -> {
                topN.add(a, b);
                return true;
            });
            scoresMap.clear();

            topN.sort();

            synchronized (writer) {
                try {
                    for (int i = 0; i < topN.size(); i++) {
                        String i_id = ((ItemVertex<String, Object[]>) ebro.getVertex(topN.getKeyAt(i))).i_ml;
                        writer.write(u_ml + "\t" + i_id + "\t" + topN.getValueAt(i));
                        writer.newLine();
                    }
                    writer.flush();
                } catch (IOException ex) {
                    Logger.getLogger(ItemBasedKNNRVF.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

        }
    }

}
