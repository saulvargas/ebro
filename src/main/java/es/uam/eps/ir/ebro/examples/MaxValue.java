package es.uam.eps.ir.ebro.examples;

import es.uam.eps.ir.ebro.Ebro;
import es.uam.eps.ir.ebro.Ebro.Vertex;
import gnu.trove.impl.Constants;
import gnu.trove.set.TDoubleSet;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TDoubleHashSet;
import gnu.trove.set.hash.TIntHashSet;
import java.util.Random;

public class MaxValue {

    public static void main(String[] args) {

        int N = 500000;
        double p = 0.0001;

        Ebro<MaxValueVertex> ebro = new Ebro<>(6, N);

        for (int i = 0; i < N; i++) {
            ebro.addVertex(new MaxValueVertex((double) i));
        }

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

            ebro.addEdge(i, j, 1.0);
        }

        ebro.run();

        TDoubleSet set = new TDoubleHashSet();
        for (int i = 0; i < N; i++) {
            MaxValueVertex v = ebro.getVertex(i);
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
        public void compute(Iterable<Double> messages) {
            double maxValue = Double.NEGATIVE_INFINITY;
            for (double v : messages) {
                if (v > maxValue) {
                    maxValue = v;
                }
            }

            if (superstep() == 0 || maxValue > value) {
                if (maxValue > value) {
                    value = maxValue;
                }
                for (int i = 0; i < edgeDestList.size(); i++) {
                    sendMessage(edgeDestList.get(i), value);
                }
            } else {
                voteToHalt();
            }
        }

    }
}
