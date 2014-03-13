/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uam.eps.ir.ebro.examples.rs;

import es.uam.eps.ir.ebro.Ebro;

/**
 *
 * @author saul
 */
public abstract class RecommendationVerticesFactory<U, I, M> {

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
        protected boolean requested;
        protected boolean pending;

        public UserVertex(U u_ml) {
            this.u_ml = u_ml;
            this.requested = false;
            this.pending = false;
        }

        public void request() {
            requested = true;
            pending = false;
        }
    }

}
