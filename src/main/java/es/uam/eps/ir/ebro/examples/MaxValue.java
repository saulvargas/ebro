package es.uam.eps.ir.ebro.examples;

import es.uam.eps.ir.ebro.Ebro;
import es.uam.eps.ir.ebro.Ebro.Vertex;
import gnu.trove.impl.Constants;
import gnu.trove.set.TDoubleSet;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TDoubleHashSet;
import gnu.trove.set.hash.TIntHashSet;
import java.util.Collection;
import java.util.Random;
import java.util.stream.IntStream;

public class MaxValue {

    public static void main(String[] args) {

        int N = 500000;
        double p = 0.0001;

        Ebro ebro = new Ebro(6, N, true, false);

        IntStream.range(0, N).forEach(i -> ebro.addVertex(new MaxValueVertex((double) i)));

        Random rnd = new Random(123456L);
        int S = (int) ((p * N) * (N - 1));

        TIntSet[] edges = new TIntSet[N];
        for (int i = 0; i < N; i++) {
            edges[i] = new TIntHashSet(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1);
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

        TDoubleSet set = new TDoubleHashSet();
        for (int i = 0; i < N; i++) {
            MaxValueVertex v = (MaxValueVertex) ebro.getVertex(i);
            set.add(v.value);
        }

        System.out.println(ebro.superstep());
        System.out.println(set.size());
        System.out.println(set.iterator().next());
    }

    public static class MaxValueVertex extends Vertex<Double> {

        public double value;

        public MaxValueVertex(double value) {
            super();
            this.value = value;
        }

        @Override
        protected void compute(Collection<Double> messages) {
            double maxValue = messages.stream().mapToDouble(Double::doubleValue).max().orElse(Double.NEGATIVE_INFINITY);

            if (superstep() == 0 || maxValue > value) {
                if (maxValue > value) {
                    value = maxValue;
                }
                edgeDestList.forEach(i_id -> {
                    sendMessage(i_id, value);
                    return true;
                });
            } else {
                voteToHalt();
            }
        }

    }
}
