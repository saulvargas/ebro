/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uam.eps.ir.ebro.examples.rs;

import java.io.BufferedWriter;
import java.util.Random;

/**
 *
 * @author saul
 */
public class PLSARVF<U, I> extends RecommendationVerticesFactory<U, I, Object[]> {

    private final int K;
    private final int numIter;

    public PLSARVF(int K, int numIter, int cutoff, BufferedWriter writer) {
        super(cutoff, writer);
        this.K = K;
        this.numIter = numIter;
    }

    @Override
    public UserVertex<U, Object[]> getUserVertex(U u) {
        return new UserVertex<U, Object[]>(u) {

            private final double[] pz_U = initVector(K);
            private double[][] qz = null;
            
            @Override
            protected void compute(Iterable<Object[]> messages) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        };
    }

    @Override
    public ItemVertex<I, Object[]> getItemVertex(I i) {
        return new ItemVertex<I, Object[]>(i) {
            
            private final double[] pIz = initVector(K);
            
            @Override
            protected void compute(Iterable<Object[]> messages) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        };
    }

    
    private static double[] initVector(int K) {
        Random random = new Random();
        final double[] v = new double[K];
        for (int k = 0; k < K; k++) {
            v[k] = random.nextDouble();
        }
        
        return v;
    }
}
