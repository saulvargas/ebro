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
package es.saulvargas.ebro.examples.rs;

import es.saulvargas.ebro.Ebro;
import es.saulvargas.ebro.Ebro.Aggregator;
import es.uam.eps.ir.ranksys.core.util.topn.TopN;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.Set;

/**
 * Vertices factory for recommendation algorithms.
 *
 * @author Saúl Vargas (saul@vargassandoval.es)
 */
public abstract class RecommendationVerticesFactory<U, I, M> {

    private final int cutoff;
    private final BufferedWriter writer;
    private final Ebro<M> ebro;
    private final Object2IntMap<U> users;
    private final Object2IntMap<I> items;

    public RecommendationVerticesFactory(Ebro<M> ebro, int cutoff, BufferedWriter writer) {
        this.cutoff = cutoff;
        this.writer = writer;
        this.ebro = ebro;
        this.users = new Object2IntOpenHashMap<>();
        this.users.defaultReturnValue(-1);
        this.items = new Object2IntOpenHashMap<>();
        this.items.defaultReturnValue(-1);
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
        ebro.addEdge(users.getInt(u), items.getInt(i));
    }

    public Set<U> getUsers() {
        return users.keySet();
    }

    public Set<I> getItems() {
        return items.keySet();
    }

    @SuppressWarnings("unchecked")
    public UserVertex<U, M> getUserVertex(U u) {
        return (UserVertex<U, M>) ebro.getVertex(users.getInt(u));
    }

    @SuppressWarnings("unchecked")
    public ItemVertex<I, M> getItemVertex(I i) {
        return (ItemVertex<I, M>) ebro.getVertex(items.getInt(i));
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
            items.values().forEach(i_id -> sendMessage(i_id, message));
        }

        @SuppressWarnings("unchecked")
        protected void printResults(Int2DoubleMap scoresMap) {
            for (int i = 0; i < edgeDestList.size(); i++) {
                int i_id = edgeDestList.getInt(i);
                scoresMap.remove(i_id);
            }

            Comparator<Entry> cmp = (e1, e2) -> {
                int c = Double.compare(e1.getDoubleValue(), e2.getDoubleValue());
                if (c != 0) {
                    return c;
                } else {
                    c = Integer.compare(e1.getIntKey(), e2.getIntKey());
                    return c;
                }
            };
            TopN<Entry> topN = new TopN<>(cutoff, cmp);
            scoresMap.int2DoubleEntrySet().forEach(topN::add);

            topN.sort();

            synchronized (writer) {
                try {
                    topN.reverseStream().forEach(e -> {
                        int i = e.getIntKey();
                        double v = e.getDoubleValue();
                        String i_id = ((ItemVertex<String, M>) ebro.getVertex(i)).i_ml;
                        try {
                            writer.write(u_ml + "\t" + i_id + "\t" + v);
                            writer.newLine();
                        } catch (IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    });
                    writer.flush();
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }

        }
    }

}
