/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uam.eps.ir.ebro.examples;

import es.uam.eps.ir.ebro.Ebro;
import es.uam.eps.ir.ebro.examples.rs.PLSARVF;
import es.uam.eps.ir.ebro.examples.rs.RecommendationVerticesFactory;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

/**
 *
 * @author saul
 */
public class Recommendations {

    public static void main(String[] args) throws Exception {
        int nthreads = 6;
        String path = "/home/saul/ceri2014/ml1M_fold1/train.data";
//        String path = "u.data";

        final Ebro ebro = new Ebro(nthreads, 5000, false, false);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter("rec"))) {
//            RecommendationVerticesFactory<String, String, Object[]> rvf = new ItemBasedKNNRVF<>(ebro, writer, 100, 50);
            RecommendationVerticesFactory<String, String, Object[]> rvf = new PLSARVF<>(ebro, 50, 200, 100, writer);

            try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    String u = tokens[0];
                    String i = tokens[1];

                    rvf.addUser(u);
                    rvf.addItem(i);

                    rvf.addEdge(u, i);
                }
            }

            for (String user : rvf.getUsers()) {
                rvf.getUserVertex(user).activate();
            }

            ebro.run();
        }
    }
}
