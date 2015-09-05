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
import es.uam.eps.ir.ebro.Ebro.Aggregator;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.BufferedWriter;
import java.util.Collection;
import java.util.Random;

/**
 * Vertices factory for the pLSA recommendation algorithm.
 *
 * @author Saúl Vargas (saul@vargassandoval.es)
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

    public PLSARVF(Ebro<Object[]> ebro, int K, int numIter, int cutoff, BufferedWriter writer) {
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
            private Int2ObjectMap<double[]> qz_Ui = null;
            private boolean waiting = false;

            @Override
            protected void compute(Collection<Object[]> messages) {
                if (superstep() < 2 * numIter) {
                    if (superstep() == 0) {

                        qz_Ui = new Int2ObjectOpenHashMap<>();
                        for (int i = 0; i < edgeDestList.size(); i++) {
                            qz_Ui.put(edgeDestList.getInt(i), new double[K]);
                        }

                    } else if (superstep() % 2 == 0) {

                        double[] normZ = normZAggr.getValue();
                        for (int z = 0; z < K; z++) {
                            pU_z[z] = 0;
                        }
                        qz_Ui.values().forEach(qz_UI -> {
                            for (int z = 0; z < K; z++) {
                                pU_z[z] += qz_UI[z];
                            }
                        });
                        for (int z = 0; z < K; z++) {
                            pU_z[z] /= normZ[z];
                        }

                    } else if (superstep() % 2 == 1) {

                        messages.forEach(m -> {
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
                        });

                        double[] normZU = new double[K];
                        qz_Ui.values().forEach(qz_UI -> {
                            for (int z = 0; z < K; z++) {
                                normZU[z] += qz_UI[z];
                            }
                        });
                        normZAggr.aggregate(normZU);

                        for (int i = 0; i < edgeDestList.size(); i++) {
                            int i_id = edgeDestList.getInt(i);
                            sendMessage(i_id, new Object[]{MessageType.USER_MAXIMIZATION, qz_Ui.get(i_id)});
                        }

                    }

                } else {

                    if (active && !waiting) {
                        sendMessageToAllItems(new Object[]{MessageType.USER_REC_REQUEST, id, pU_z});
                        waiting = true;
                    }

                    Int2DoubleMap scoresMap = new Int2DoubleOpenHashMap();
                    messages.forEach(m -> {
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
                    });

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
            protected void compute(Collection<Object[]> messages) {
                if (superstep() < 2 * numIter) {

                    if (superstep() % 2 == 0) {
                        if (superstep() > 0) {
                            for (int z = 0; z < K; z++) {
                                pIz[z] = 0.0;
                            }
                        }

                        messages.forEach(m -> {
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
                        });

                        for (int i = 0; i < edgeDestList.size(); i++) {
                            int u_id = edgeDestList.getInt(i);
                            sendMessage(u_id, new Object[]{MessageType.ITEM_EXPECTATION, id, pIz});
                        }
                    }

                } else {

                    messages.parallelStream().forEach(m -> {
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
                    });

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
