/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uam.eps.ir.ebro.examples.rs;

import es.uam.eps.ir.ebro.Ebro;
import es.uam.eps.ir.ebro.Ebro.Aggregator;
import gnu.trove.impl.Constants;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.io.BufferedWriter;
import java.util.Random;

/**
 *
 * @author saul
 */
public class PLSARVF<U, I> extends RecommendationVerticesFactory<U, I, Object[]> {

    private final int K;
    private final int numIter;
    private final NormZAggr normZAggr;

    private enum MessageType {

        ITEM_EXPECTATION,
        USER_MAXIMIZATION,
        USER_REC_REQUEST,
        ITEM_REC_RESPONSE
    }

    public PLSARVF(Ebro ebro, int K, int numIter, int cutoff, BufferedWriter writer) {
        super(ebro, cutoff, writer);
        this.K = K;
        this.numIter = numIter;

        this.normZAggr = new NormZAggr(K);
        addAggregator(normZAggr);
    }

    @Override
    public UserVertex<U, Object[]> createUserVertex(U u) {
        return new UserVertex<U, Object[]>(u) {

            private final double[] pU_z = initVector(K);
            private TIntObjectMap<double[]> qz_Ui = null;
            private boolean waiting = false;

            @Override
            protected void compute(Iterable<Object[]> messages) {
                if (superstep() < 2 * numIter) {
                    if (superstep() == 0) {

                        qz_Ui = new TIntObjectHashMap<>();
                        for (int i = 0; i < edgeDestList.size(); i++) {
                            qz_Ui.put(edgeDestList.getQuick(i), new double[K]);
                        }

                    } else if (superstep() % 2 == 0) {

                        double[] normZ = normZAggr.getValue();
                        for (int z = 0; z < K; z++) {
                            pU_z[z] = 0;
                        }
                        for (double[] qz_UI : qz_Ui.valueCollection()) {
                            for (int z = 0; z < K; z++) {
                                pU_z[z] += qz_UI[z];
                            }
                        }
                        for (int z = 0; z < K; z++) {
                            pU_z[z] /= normZ[z];
                        }

                    } else if (superstep() % 2 == 1) {

                        for (Object[] m : messages) {
                            switch ((MessageType) m[0]) {
                                case ITEM_EXPECTATION:
                                    int i_id = (Integer) m[1];
                                    double[] pIz = (double[]) m[2];

                                    double[] qz_UI = qz_Ui.get(i_id);
                                    double norm = 0.0;
                                    for (int z = 0; z < K; z++) {
                                        qz_UI[z] = pU_z[z] * pIz[z];
                                        norm += qz_UI[z];
                                    }
                                    for (int z = 0; z < K; z++) {
                                        qz_UI[z] /= norm;
                                    }
                                    break;
                                default:
                                    break;
                            }
                        }

                        double[] normZU = new double[K];
                        for (double[] qz_UI : qz_Ui.valueCollection()) {
                            for (int z = 0; z < K; z++) {
                                normZU[z] += qz_UI[z];
                            }
                        }
                        normZAggr.aggregate(normZU);

                        for (int i = 0; i < edgeDestList.size(); i++) {
                            int i_id = edgeDestList.getQuick(i);
                            sendMessage(i_id, new Object[]{MessageType.USER_MAXIMIZATION, qz_Ui.get(i_id)});
                        }

                    }

                } else {

                    if (active && !waiting) {
                        sendMessageToAllItems(new Object[]{MessageType.USER_REC_REQUEST, id, pU_z});
                        waiting = true;
                    }

                    TIntDoubleMap scoresMap = new TIntDoubleHashMap(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1, 0.0);
                    for (Object[] m : messages) {
                        switch ((MessageType) m[0]) {
                            case ITEM_REC_RESPONSE:
                                int i_id = (Integer) m[1];
                                double score = (Double) m[2];

                                scoresMap.put(i_id, score);

                                waiting = false;
                                break;
                            default:
                                break;
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
        };
    }

    @Override
    public ItemVertex<I, Object[]> createItemVertex(I i) {
        return new ItemVertex<I, Object[]>(i) {

            private final double[] pIz = initVector(K);

            @Override
            protected void compute(Iterable<Object[]> messages) {
                if (superstep() < 2 * numIter) {

                    if (superstep() % 2 == 0) {
                        if (superstep() > 0) {
                            for (int z = 0; z < K; z++) {
                                pIz[z] = 0.0;
                            }
                        }

                        for (Object[] m : messages) {
                            switch ((MessageType) m[0]) {
                                case USER_MAXIMIZATION:
                                    double[] qz_UI = (double[]) m[1];

                                    for (int z = 0; z < K; z++) {
                                        pIz[z] += qz_UI[z];
                                    }
                                    break;
                                default:
                                    break;
                            }
                        }

                        for (int i = 0; i < edgeDestList.size(); i++) {
                            int u_id = edgeDestList.getQuick(i);
                            sendMessage(u_id, new Object[]{MessageType.ITEM_EXPECTATION, id, pIz});
                        }
                    }

                } else {

                    for (Object[] m : messages) {
                        switch ((MessageType) m[0]) {
                            case USER_REC_REQUEST:
                                int u_id = (Integer) m[1];
                                double[] pz_U = (double[]) m[2];

                                double score = 0;
                                for (int z = 0; z < K; z++) {
                                    score += pz_U[z] * pIz[z];
                                }

                                sendMessage(u_id, new Object[]{MessageType.ITEM_REC_RESPONSE, id, score});
                                break;
                            default:
                                break;
                        }
                    }

                    voteToHalt();

                }
            }
        };
    }

    private static double[] initVector(int K) {
        Random random = new Random();
        final double[] v = new double[K];
        for (int z = 0; z < K; z++) {
            v[z] = random.nextDouble();
        }

        return v;
    }

    private static class NormZAggr extends Aggregator<double[]> {

        private final int K;

        public NormZAggr(int K) {
            this.K = K;
        }

        @Override
        protected double[] init() {
            return new double[K];
        }

        @Override
        protected double[] aggregate(double[] t1, double[] t) {
            for (int z = 0; z < K; z++) {
                t1[z] += t[z];
            }

            return t1;
        }

    }

}
