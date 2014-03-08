/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uam.eps.ir.ebro.examples;

import es.uam.eps.ir.ebro.Ebro;
import es.uam.eps.ir.ebro.Ebro.Vertex;
import gnu.trove.impl.Constants;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.procedure.TIntDoubleProcedure;
import java.io.BufferedReader;
import java.io.FileReader;

/**
 *
 * @author saul
 */
public class ItemBasedKNNRecommender {

    public static void main(String[] args) throws Exception {
        final Ebro<Vertex> ebro = new Ebro<>(4, 5000);

        TIntIntMap users = new TIntIntHashMap(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1, -1);
        TIntIntMap items = new TIntIntHashMap(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1, -1);

        try (BufferedReader reader = new BufferedReader(new FileReader("u.data"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split("\t");
                int u = Integer.parseInt(tokens[0]);
                int i = Integer.parseInt(tokens[1]);

                if (!users.containsKey(u)) {
                    users.put(u, ebro.addVertex(new UserVertex(u)));
                }
                if (!items.containsKey(i)) {
                    items.put(i, ebro.addVertex(new ItemVertex(i)));
                }
                
                ebro.addEdge(users.get(u), items.get(i), 1.0);
                ebro.addEdge(items.get(i), users.get(u), 1.0);
            }
        }

        ebro.run();

        System.out.println(ebro.superstep());
        TIntDoubleMap recommendation = ((UserVertex) ebro.getVertex(0)).recommendation;
        recommendation.forEachEntry(new TIntDoubleProcedure() {

            @Override
            public boolean execute(int i, double s) {
                System.out.println(i + "\t" + s);
                return true;
            }
        });
    }

    public static class ItemVertex extends Vertex<int[]> {

        private final int i_id;

        public ItemVertex(int itemID) {
            this.i_id = itemID;
        }

        @Override
        protected void compute(Iterable<int[]> messages) {
            for (int[] m : messages) {
                switch (m[0]) {
                    case 0:
                        int u_id = m[1];
                        for (int i = 0; i < edgeDestList.size(); i++) {
                            int v_id = edgeDestList.get(i);
                            if (u_id != v_id) {
                                sendMessage(v_id, new int[]{1, u_id, i_id});
                            }
                        }
                        break;
                }
            }
            voteToHalt();
        }

    }

    public static class UserVertex extends Vertex<int[]> {

        private final int u_id;
        private boolean requested;
        private final TIntDoubleMap recommendation;

        public UserVertex(int userID) {
            this.u_id = userID;
            this.requested = false;
            this.recommendation = new TIntDoubleHashMap(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1, 0.0);
        }

        @Override
        protected void compute(Iterable<int[]> messages) {
            if (!requested) {
                for (int i = 0; i < edgeDestList.size(); i++) {
                    sendMessage(edgeDestList.get(i), new int[]{0, u_id});
                }

                requested = true;
            }

            for (int[] m : messages) {
                switch (m[0]) {
                    case 1:
                        int v_id = m[1];
                        int i_id = m[2];
                        for (int i = 0; i < edgeDestList.size(); i++) {
                            int j_id = edgeDestList.get(i);
                            if (i_id != j_id) {
                                sendMessage(v_id, new int[]{2, j_id});
                            }
                        }
                        break;
                    case 2:
                        int j_id = m[1];
                        recommendation.adjustOrPutValue(j_id, 1, 1);
                        break;
                }
            }
            voteToHalt();
        }

    }
    
//        int u0 = ebro.addVertex(new UserVertex(0));
//        int u1 = ebro.addVertex(new UserVertex(1));
//        int u2 = ebro.addVertex(new UserVertex(2));
//        int u3 = ebro.addVertex(new UserVertex(3));
//        int i1 = ebro.addVertex(new ItemVertex(1));
//        int i2 = ebro.addVertex(new ItemVertex(2));
//        int i3 = ebro.addVertex(new ItemVertex(3));
//        int i4 = ebro.addVertex(new ItemVertex(4));
//        int i5 = ebro.addVertex(new ItemVertex(5));
//
//        ebro.addEdge(u0, i1, 1.0);
//        ebro.addEdge(i1, u0, 1.0);
//        ebro.addEdge(u0, i2, 1.0);
//        ebro.addEdge(i2, u0, 1.0);
//        ebro.addEdge(u0, i3, 1.0);
//        ebro.addEdge(i3, u0, 1.0);
//        ebro.addEdge(i1, u1, 1.0);
//        ebro.addEdge(u1, i1, 1.0);
//        ebro.addEdge(i2, u2, 1.0);
//        ebro.addEdge(u2, i2, 1.0);
//        ebro.addEdge(i3, u2, 1.0);
//        ebro.addEdge(u2, i3, 1.0);
//        ebro.addEdge(i3, u3, 1.0);
//        ebro.addEdge(u3, i3, 1.0);
//        ebro.addEdge(u1, i4, 1.0);
//        ebro.addEdge(i4, u1, 1.0);
//        ebro.addEdge(u1, i5, 1.0);
//        ebro.addEdge(i5, u1, 1.0);
//        ebro.addEdge(u2, i4, 1.0);
//        ebro.addEdge(i4, u2, 1.0);
//        ebro.addEdge(u3, i5, 1.0);
//        ebro.addEdge(i5, u3, 1.0);

}
