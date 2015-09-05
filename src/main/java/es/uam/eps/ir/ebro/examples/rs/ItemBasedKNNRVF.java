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
package es.uam.eps.ir.ebro.examples.rs;

import es.uam.eps.ir.ebro.Ebro;
import es.uam.eps.ir.ranksys.core.util.topn.TopN;
import it.unimi.dsi.fastutil.ints.AbstractInt2DoubleMap.BasicEntry;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.BufferedWriter;
import java.util.Collection;
import java.util.Comparator;

/**
 * Vertices factory for the Item-based kNN recommendation algorithm.
 *
 * @author Saúl Vargas (saul@vargassandoval.es)
 */
public class ItemBasedKNNRVF<U, I> extends RecommendationVerticesFactory<U, I, Object[]> {

    private final int N;

    private enum MessageType {

        USER_REC_REQUEST, // (u)        SR
        ITEM_SIM_BEGIN, // (i)          SR
        USER_SIM_FORWARD, // ([i, n])     SR
        ITEM_REC_RESPONSE,  // ([i, s]) SR
    }

    public ItemBasedKNNRVF(Ebro<Object[]> ebro, BufferedWriter writer, int cutoff, int N) {
        super(ebro, cutoff, writer);
        this.N = N;
    }

    @Override
    public UserVertex<U, Object[]> createUserVertex(U u) {
        return new ItemBasedKNNUserVertex(u);
    }

    @Override

    public ItemVertex<I, Object[]> createItemVertex(I i) {
        return new ItemBasedKNNItemVertex(i);
    }

    private class ItemBasedKNNUserVertex extends UserVertex<U, Object[]> {

        private boolean waiting = false;

        public ItemBasedKNNUserVertex(U u_ml) {
            super(u_ml);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected void compute(Collection<Object[]> messages) {
            if (active && !waiting) {
                for (int i = 0; i < edgeDestList.size(); i++) {
                    int i_id = edgeDestList.getInt(i);
                    sendMessage(i_id, new Object[]{MessageType.USER_REC_REQUEST, id});
                }
                waiting = true;
            }

            Int2DoubleOpenHashMap scoresMap = new Int2DoubleOpenHashMap();
            IntArrayList edges = new IntArrayList();
            IntArrayList sizes = new IntArrayList();
            messages.forEach(m -> {
                switch ((MessageType) m[0]) {
                    case ITEM_SIM_BEGIN:
                        int i_id = (Integer) m[1];
                        int ni = (Integer) m[2];
                        edges.add(i_id);
                        sizes.add(ni);
                        break;
                    case ITEM_REC_RESPONSE:
                        TopN<Entry> sim = (TopN<Entry>) m[1];

                        sim.forEach(e -> scoresMap.addTo(e.getIntKey(), e.getDoubleValue()));
                        waiting = false;
                        break;
                }
            });

            if (!edges.isEmpty()) {
                for (int i = 0; i < edgeDestList.size(); i++) {
                    int j_id = edgeDestList.getInt(i);
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

    }

    private class ItemBasedKNNItemVertex extends ItemVertex<I, Object[]> {

        private boolean active = true;
        private boolean waiting = false;
        private final IntArrayList users = new IntArrayList();

        public ItemBasedKNNItemVertex(I i_ml) {
            super(i_ml);
        }

        @Override
        protected void compute(Collection<Object[]> messages) {
            if (active && !waiting) {
                for (int i = 0; i < edgeDestList.size(); i++) {
                    int v_id = edgeDestList.getInt(i);
                    sendMessage(v_id, new Object[]{MessageType.ITEM_SIM_BEGIN, id, edgeDestList.size()});
                }
                waiting = true;
            }

            Int2IntOpenHashMap intersection = new Int2IntOpenHashMap();
            Int2IntMap count = new Int2IntOpenHashMap();
            messages.forEach(m -> {
                switch ((MessageType) m[0]) {
                    case USER_REC_REQUEST:
                        int u_id = (Integer) m[1];
                        users.add(u_id);
                        break;
                    case USER_SIM_FORWARD:
                        IntArrayList edges = (IntArrayList) m[1];
                        IntArrayList sizes = (IntArrayList) m[2];
                        for (int i = 0; i < edges.size(); i++) {
                            int j_id = edges.getInt(i);
                            int nj = sizes.getInt(i);
                            if (j_id != id) {
                                intersection.addTo(j_id, 1);
                                count.put(j_id, nj);
                            }
                        }

                        waiting = false;
                        break;
                }
            });

            if (active && !waiting) {
                Comparator<Entry> cmp = (e1, e2) -> {
                    int c = Double.compare(e1.getDoubleValue(), e2.getDoubleValue());
                    if (c != 0) {
                        return c;
                    } else {
                        c = Integer.compare(e1.getIntKey(), e2.getIntKey());
                        return c;
                    }
                };
                final TopN<Entry> sim = new TopN<>(N, cmp);

                final double ni = edgeDestList.size();
                intersection.int2IntEntrySet().forEach(e -> {
                    int j_id = e.getIntKey();
                    int i1 = e.getIntValue();
                    double v = i1 / (double) (ni + count.get(j_id) - i1);
                    sim.add(new BasicEntry(j_id, v));
                });

                users.forEach(u_id -> {
                    sendMessage(u_id, new Object[]{MessageType.ITEM_REC_RESPONSE, sim});
                });

                users.clear();

                active = false;
            }

            voteToHalt();
        }

    }
}
