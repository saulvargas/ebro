/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uam.eps.ir.ebro.examples.rs;

import es.uam.eps.ir.utils.dstructs.TIntDoubleTopN;
import gnu.trove.impl.Constants;
import gnu.trove.list.TIntList;
import gnu.trove.list.linked.TIntLinkedList;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.procedure.TIntDoubleProcedure;
import gnu.trove.procedure.TIntProcedure;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author saul
 */
public class ItemBasedKNNRVF<U, I> extends RecommendationVerticesFactory<U, I, Object[]> {

    private final BufferedWriter writer;

    public ItemBasedKNNRVF(BufferedWriter writer) {
        this.writer = writer;
    }

    @Override
    public UserVertex<U, Object[]> getUserVertex(U u) {
        return new UserVertex<U, Object[]>(u) {

            private boolean ready = false;
            private boolean waiting = false;
            private TIntDoubleMap auxMap = null;

            @Override
            protected void compute(Iterable<Object[]> messages) {
                if (requested && !ready && !waiting) {
                    for (int i = 0; i < edgeDestList.size(); i++) {
                        int i_id = edgeDestList.get(i);
                        sendMessage(i_id, new Object[]{0, id});
                    }
                    waiting = true;
                }
                if (!requested) {
                    ready = true;
                }

                for (Object[] m : messages) {
                    switch ((Integer) m[0]) {
                        case 1:
                            int i_id = (Integer) m[1];
                            sendMessage(i_id, new Object[]{2, edgeDestList});
                            break;
                        case 3:
                            TIntDoubleMap sim = (TIntDoubleMap) m[1];

                            if (auxMap == null) {
                                auxMap = new TIntDoubleHashMap();
                            }

                            sim.forEachEntry(new TIntDoubleProcedure() {

                                @Override
                                public boolean execute(int j_id, double sim) {
                                    auxMap.adjustOrPutValue(j_id, sim, sim);
                                    return true;
                                }
                            });
                            ready = true;
                            break;
                    }
                }

                if (ready && requested) {
                    for (int i = 0; i < edgeDestList.size(); i++) {
                        int i_id = edgeDestList.get(i);
                        auxMap.remove(i_id);
                    }

                    final TIntDoubleTopN topN = new TIntDoubleTopN(100);
                    auxMap.forEachEntry(new TIntDoubleProcedure() {

                        @Override
                        public boolean execute(int a, double b) {
                            topN.add(a, b);
                            return true;
                        }
                    });
                    auxMap.clear();
                    auxMap = null;

                    topN.sort();

                    synchronized (writer) {
                        try {
                            for (int i = 0; i < topN.size(); i++) {
                                String i_id = ((ItemVertex<String, Object[]>) ebro.getVertex(topN.getKeyAt(i))).i_ml;
                                writer.write(u_ml + "\tQ0\t" + i_id + "\t" + (i + 1) + "\t" + topN.getValueAt(i) + "\tr");
                                writer.newLine();
                            }
                            writer.flush();
                        } catch (IOException ex) {
                            Logger.getLogger(ItemBasedKNNRVF.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                }

                if (ready) {
                    voteToHalt();
                }
            }

        };
    }

    @Override
    public ItemVertex<I, Object[]> getItemVertex(I i) {
        return new ItemVertex<I, Object[]>(i) {

            private final TIntDoubleMap sim = new TIntDoubleHashMap(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1, 0.0);
            private boolean ready = false;
            private boolean waiting = false;
            private final TIntList users = new TIntLinkedList();

            @Override
            protected void compute(Iterable<Object[]> messages) {
                if (!ready && !waiting) {
                    for (int i = 0; i < edgeDestList.size(); i++) {
                        int v_id = edgeDestList.get(i);
                        sendMessage(v_id, new Object[]{1, id});
                    }
                    waiting = true;
                }

                for (Object[] m : messages) {
                    switch ((Integer) m[0]) {
                        case 0:
                            int u_id = (Integer) m[1];
                            users.add(u_id);
                            break;
                        case 2:
                            TIntList list = (TIntList) m[1];
                            list.forEach(new TIntProcedure() {

                                @Override
                                public boolean execute(int j_id) {
                                    sim.adjustOrPutValue(j_id, 1, 1);
                                    return true;
                                }
                            });
                            waiting = false;
                            ready = true;
                            break;
                    }
                }

                if (ready) {
                    users.forEach(new TIntProcedure() {

                        @Override
                        public boolean execute(int u_id) {
                            sendMessage(u_id, new Object[]{3, sim});
                            return true;
                        }
                    });
                    users.clear();
                    voteToHalt();
                }

            }
        };
    }
}
