/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uam.eps.ir.ebro.examples.rs;

import es.uam.eps.ir.ebro.Ebro;
import es.uam.eps.ir.utils.dstructs.TIntDoubleTopN;
import gnu.trove.impl.Constants;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import java.io.BufferedWriter;

/**
 *
 * @author saul
 */
public class ItemBasedKNNRVF<U, I> extends RecommendationVerticesFactory<U, I, Object[]> {

    private final int N;

    private enum MessageType {

        USER_REC_REQUEST, // (u)        SR
        ITEM_SIM_BEGIN, // (i)          SR
        USER_SIM_FORWARD, // ([i, n])     SR
        ITEM_REC_RESPONSE,  // ([i, s]) SR
    }

    public ItemBasedKNNRVF(Ebro ebro, BufferedWriter writer, int cutoff, int N) {
        super(ebro, cutoff, writer);
        this.N = N;
    }

    @Override
    public UserVertex<U, Object[]> createUserVertex(U u) {
        return new UserVertex<U, Object[]>(u) {

            private boolean waiting = false;

            @Override
            protected void compute(Iterable<Object[]> messages) {
                if (active && !waiting) {
                    for (int i = 0; i < edgeDestList.size(); i++) {
                        int i_id = edgeDestList.getQuick(i);
                        sendMessage(i_id, new Object[]{MessageType.USER_REC_REQUEST, id});
                    }
                    waiting = true;
                }

                TIntDoubleMap scoresMap = new TIntDoubleHashMap(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1, 0.0);
                TIntArrayList edges = new TIntArrayList();
                TIntArrayList sizes = new TIntArrayList();
                for (Object[] m : messages) {
                    switch ((MessageType) m[0]) {
                        case ITEM_SIM_BEGIN:
                            int i_id = (Integer) m[1];
                            int ni = (Integer) m[2];
                            edges.add(i_id);
                            sizes.add(ni);
                            break;
                        case ITEM_REC_RESPONSE:
                            TIntDoubleTopN sim = (TIntDoubleTopN) m[1];

                            for (int i = 0; i < sim.size(); i++) {
                                scoresMap.adjustOrPutValue(sim.getKeyAt(i), sim.getValueAt(i), sim.getValueAt(i));
                            }
                            waiting = false;
                            break;
                    }
                }

                if (!edges.isEmpty()) {
                    for (int i = 0; i < edgeDestList.size(); i++) {
                        int j_id = edgeDestList.getQuick(i);
                        sendMessage(j_id, new Object[]{MessageType.USER_SIM_FORWARD, edges, sizes});
                    }
                }

                if (active && !waiting) {
                    printResults(scoresMap);
                    scoresMap.clear();

                    active = false;
                }

                if (!active) {
                    voteToHalt();
                }
            }

        };
    }

    @Override
    public ItemVertex<I, Object[]> createItemVertex(I i) {
        return new ItemVertex<I, Object[]>(i) {

            private boolean active = true;
            private boolean waiting = false;
            private final TIntArrayList users = new TIntArrayList();

            @Override
            protected void compute(Iterable<Object[]> messages) {
                if (active && !waiting) {
                    for (int i = 0; i < edgeDestList.size(); i++) {
                        int v_id = edgeDestList.getQuick(i);
                        sendMessage(v_id, new Object[]{MessageType.ITEM_SIM_BEGIN, id, edgeDestList.size()});
                    }
                    waiting = true;
                }

                final TIntIntMap intersection = new TIntIntHashMap(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1, 0);
                final TIntIntMap count = new TIntIntHashMap(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1, 0);
                for (Object[] m : messages) {
                    switch ((MessageType) m[0]) {
                        case USER_REC_REQUEST:
                            int u_id = (Integer) m[1];
                            users.add(u_id);
                            break;
                        case USER_SIM_FORWARD:
                            TIntArrayList edges = (TIntArrayList) m[1];
                            TIntArrayList sizes = (TIntArrayList) m[2];
                            for (int i = 0; i < edges.size(); i++) {
                                int j_id = edges.getQuick(i);
                                int nj = sizes.getQuick(i);
                                if (j_id != id) {
                                    intersection.adjustOrPutValue(j_id, 1, 1);
                                    count.put(j_id, nj);
                                }
                            }

                            waiting = false;
                            break;
                    }
                }

                if (active && !waiting) {
                    final TIntDoubleTopN sim = new TIntDoubleTopN(N);

                    final double ni = edgeDestList.size();
                    intersection.forEachEntry((j_id, i1) -> {
                        sim.add(j_id, i1 / (double) (ni + count.get(j_id) - i1));
                        return true;
                    });

                    users.forEach(u_id -> {
                        sendMessage(u_id, new Object[]{MessageType.ITEM_REC_RESPONSE, sim});
                        return true;
                    });

                    users.clear();

                    active = false;
                }

                voteToHalt();
            }
        };
    }
}
