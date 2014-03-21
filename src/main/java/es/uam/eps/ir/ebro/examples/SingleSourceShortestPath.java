package es.uam.eps.ir.ebro.examples;

import es.uam.eps.ir.ebro.Ebro;
import es.uam.eps.ir.ebro.Ebro.Vertex;
import gnu.trove.impl.Constants;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import java.util.Random;
import java.util.stream.Stream;

public class SingleSourceShortestPath {

    public static void main(String[] args) {

        int N = 500000;
        double p = 0.0001;

        Ebro ebro = new Ebro(6, N, true, true);

        for (int i = 0; i < N; i++) {
            ebro.addVertex(new SSSPVertex(0, Double.POSITIVE_INFINITY));
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
        protected void compute(Stream<Double> messages) {
            double minDist = source == id ? 0.0 : Double.POSITIVE_INFINITY;
            minDist = messages.mapToDouble(Double::doubleValue).min().orElse(minDist);

            if (minDist < dist) {
                dist = minDist;
                for (int i = 0; i < edgeDestList.size(); i++) {
                    sendMessage(edgeDestList.getQuick(i), dist + edgeWeightList.getQuick(i));
                }
            }
            voteToHalt();
        }

    }
}
