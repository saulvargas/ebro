/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uam.eps.ir.ebro.examples;

import es.uam.eps.ir.ebro.Ebro;
import es.uam.eps.ir.ebro.examples.rs.PLSARVF;
import es.uam.eps.ir.ebro.examples.rs.ItemBasedKNNRVF;
import es.uam.eps.ir.ebro.examples.rs.RecommendationVerticesFactory;
import es.uam.eps.ir.ebro.examples.rs.RecommendationVerticesFactory.UserVertex;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 *
 * @author saul
 */
public class Recommendations {

    public static void main(String[] args) throws Exception {
        String fileIn = null;
        String dirOut = null;
        int nthreads = 6;

        Map<String, BiFunction<Ebro, BufferedWriter, RecommendationVerticesFactory<String, String, Object[]>>> rvfMap = new HashMap<>();

        rvfMap.put("ib", (ebro, writer) -> new ItemBasedKNNRVF<>(ebro, writer, 100, 50));
        rvfMap.put("plsa", (ebro, writer) -> new PLSARVF<>(ebro, 50, 200, 100, writer));

        rvfMap.forEach((name, supplier) -> {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(dirOut + name))) {
                final Ebro ebro = new Ebro(nthreads, 5000, false, false);

                RecommendationVerticesFactory<String, String, Object[]> rvf = supplier.apply(ebro, writer);
                
                try (BufferedReader reader = new BufferedReader(new FileReader(fileIn))) {

                    reader.lines().map(l -> l.split("\t")).forEach(tokens -> {
                        String u = tokens[0];
                        String i = tokens[1];

                        rvf.addUser(u);
                        rvf.addItem(i);

                        rvf.addEdge(u, i);
                    });
                }

                rvf.getUsers().stream().map(rvf::getUserVertex).forEach(UserVertex::activate);

                ebro.run();
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        });
    }
}
