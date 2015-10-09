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
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import static java.lang.Math.max;
import java.util.Collection;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * Example: find the maximum value of each connected component
 * of a graph.
 *
 * @author Saúl Vargas (saul@vargassandoval.es)
 */
public class MaxValue {

    public static void main(String[] args) {

        int N = 500000;
        double p = 0.0001;

        Ebro<Double> ebro = new Ebro<>(N, true, false);

        IntStream.range(0, N).forEach(i -> ebro.addVertex(new MaxValueVertex((double) i)));

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

            ebro.addEdge(i, j);
        }

        ebro.run();

        DoubleSet set = new DoubleOpenHashSet();
        for (int i = 0; i < N; i++) {
            MaxValueVertex v = (MaxValueVertex) ebro.getVertex(i);
            set.add(v.value);
        }

        System.out.println(ebro.superstep());
        System.out.println(set.size());
        System.out.println(set.iterator().nextDouble());
    }

    public static class MaxValueVertex extends Vertex<Double> {

        public Double value;

        public MaxValueVertex(double value) {
            super();
            this.value = value;
        }

        @Override
        protected void compute(Collection<Double> messages) {
            double maxValue = messages.stream().mapToDouble(Double::doubleValue)
                    .max().orElse(Double.NEGATIVE_INFINITY);

            if (superstep() == 0 || maxValue > value) {
                value = max(value, maxValue);
                edgeDestList.forEach(i_id -> sendMessage(i_id, value));
            } else {
                voteToHalt();
            }
        }

    }
}
