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
package es.saulvargas.ebro.examples;

import es.saulvargas.ebro.Ebro;
import es.saulvargas.ebro.Ebro.Vertex;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Collection;
import java.util.Random;

/**
 * Example: single-source shortest path algorithm.
 *
 * @author Saúl Vargas (saul@vargassandoval.es)
 */
public class SingleSourceShortestPath {

    public static void main(String[] args) {

        int N = 500000;
        double p = 0.0001;

        Ebro<Double> ebro = new Ebro<>(N, true, true);

        for (int i = 0; i < N; i++) {
            ebro.addVertex(new SSSPVertex(0, Double.POSITIVE_INFINITY));
        }

        Random rnd = new Random(123456L);
        int S = (int) ((p * N) * (N - 1));

        IntSet[] edges = new IntSet[N];
        for (int i = 0; i < N; i++) {
            edges[i] = new IntOpenHashSet();
        }

        for (int s = 0; s < S; s++) {
            int i = rnd.nextInt(N);
            int j = rnd.nextInt(N);
            while (i == j || edges[i].contains(j)) {
                j = rnd.nextInt(N);
            }
            edges[i].add(j);

            ebro.addEdge(i, j, 1.0);
        }

        ebro.run();

        double avgDist = 0.0;
        int n = 0;
        for (int i = 0; i < N; i++) {
            SSSPVertex v = (SSSPVertex) ebro.getVertex(i);
            if (!Double.isInfinite(v.dist)) {
                avgDist += v.dist;
                n++;
            }
        }
        avgDist /= n;

        System.out.println(ebro.superstep());
        System.out.println(avgDist);
        System.out.println(n + " / " + N + " = " + (n / (double) N));
    }

    public static class SSSPVertex extends Vertex<Double> {

        private final int source;
        private double dist;

        public SSSPVertex(int source, double value) {
            super();
            this.source = source;
            this.dist = value;
        }

        @Override
        protected void compute(Collection<Double> messages) {
            double minDist = source == id ? 0.0 : Double.POSITIVE_INFINITY;
            minDist = messages.stream().mapToDouble(Double::doubleValue).min().orElse(minDist);

            if (minDist < dist) {
                dist = minDist;
                for (int i = 0; i < edgeDestList.size(); i++) {
                    sendMessage(edgeDestList.getInt(i), dist + edgeWeightList.getDouble(i));
                }
            }
            voteToHalt();
        }

    }
}
