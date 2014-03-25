/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uam.eps.ir.ebro.examples;

import es.uam.eps.ir.ebro.Ebro;
import es.uam.eps.ir.ebro.examples.rs.PLSARVF;
import es.uam.eps.ir.ebro.examples.rs.RecommendationVerticesFactory;
import es.uam.eps.ir.ebro.examples.rs.RecommendationVerticesFactory.UserVertex;
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
//        String path = "/home/saul/ceri2014/ml1M_fold1/train.data";
        String path = "u.data";
//        String path = "/datacthulhu/saul/MSD/msd-song/train1.data";

        final Ebro ebro = new Ebro(nthreads, 5000, false, false);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter("rec"))) {
//            RecommendationVerticesFactory<String, String, Object[]> rvf = new ItemBasedKNNRVF<>(ebro, writer, 100, 50);
            RecommendationVerticesFactory<String, String, Object[]> rvf = new PLSARVF<>(ebro, 50, 200, 100, writer);

            try (BufferedReader reader = new BufferedReader(new FileReader(path))) {

                reader.lines().map(l -> l.split("\t")).forEach(tokens -> {
                    String u = tokens[0];
                    String i = tokens[1];

                    rvf.addUser(u);
                    rvf.addItem(i);

                    rvf.addEdge(u, i);
                });
            }

//            HashMap<Integer, Double> map = new HashMap<>();
//            map.
//            
            rvf.getUsers().stream().map(rvf::getUserVertex).forEach(UserVertex::activate);

            ebro.run();
        }
    }
}
