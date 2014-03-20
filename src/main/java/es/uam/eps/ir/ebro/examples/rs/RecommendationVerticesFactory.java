/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uam.eps.ir.ebro.examples.rs;

import es.uam.eps.ir.ebro.Ebro;
import es.uam.eps.ir.utils.dstructs.TIntDoubleTopN;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.procedure.TIntDoubleProcedure;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author saul
 */
public abstract class RecommendationVerticesFactory<U, I, M> {

    private final int cutoff;
    private final BufferedWriter writer;

    public RecommendationVerticesFactory(int cutoff, BufferedWriter writer) {
        this.cutoff = cutoff;
        this.writer = writer;
    }

    public abstract UserVertex<U, M> getUserVertex(U u);

    public abstract ItemVertex<I, M> getItemVertex(I i);

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

        protected void printResults(TIntDoubleMap scoresMap ){
            for (int i = 0; i < edgeDestList.size(); i++) {
                int i_id = edgeDestList.getQuick(i);
                scoresMap.remove(i_id);
            }

            final TIntDoubleTopN topN = new TIntDoubleTopN(cutoff);
            scoresMap.forEachEntry(new TIntDoubleProcedure() {

                @Override
                public boolean execute(int a, double b) {
                    topN.add(a, b);
                    return true;
                }
            });
            scoresMap.clear();

            topN.sort();

            synchronized (writer) {
                try {
                    for (int i = 0; i < topN.size(); i++) {
                        String i_id = ((ItemVertex<String, Object[]>) ebro.getVertex(topN.getKeyAt(i))).i_ml;
                        writer.write(u_ml + "\t" + i_id + "\t" + topN.getValueAt(i));
                        writer.newLine();
                    }
                    writer.flush();
                } catch (IOException ex) {
                    Logger.getLogger(ItemBasedKNNRVF.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

        }
    }

}
