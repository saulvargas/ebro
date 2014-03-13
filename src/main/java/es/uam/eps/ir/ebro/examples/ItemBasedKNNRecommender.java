/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uam.eps.ir.ebro.examples;

import es.uam.eps.ir.ebro.Ebro;
import es.uam.eps.ir.ebro.Ebro.Vertex;
import es.uam.eps.ir.ebro.examples.rs.ItemBasedKNNRVF;
import es.uam.eps.ir.ebro.examples.rs.RecommendationVerticesFactory;
import es.uam.eps.ir.ebro.examples.rs.RecommendationVerticesFactory.UserVertex;
import gnu.trove.impl.Constants;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

/**
 *
 * @author saul
 */
public class ItemBasedKNNRecommender {

    public static void main(String[] args) throws Exception {
        int nthreads = 6;
        String path = "u.data";

        final Ebro<Vertex> ebro = new Ebro<>(nthreads, 5000, false, false);
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("rec"))) {
            RecommendationVerticesFactory<String, String, Object[]> rvf = new ItemBasedKNNRVF<>(writer);
            TObjectIntMap<String> users = new TObjectIntHashMap<>(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1);
            TObjectIntMap<String> items = new TObjectIntHashMap<>(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1);

            try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    String u = tokens[0];
                    String i = tokens[1];

                    if (!users.containsKey(u)) {
                        users.put(u, ebro.addVertex(rvf.getUserVertex(u)));
                    }
                    if (!items.containsKey(i)) {
                        items.put(i, ebro.addVertex(rvf.getItemVertex(i)));
                    }

                    ebro.addEdge(users.get(u), items.get(i));
                }
            }
            items.clear();

            for (int id : users.values()) {
                ((UserVertex) ebro.getVertex(id)).request();
            }

            ebro.run();
        }
    }
}
