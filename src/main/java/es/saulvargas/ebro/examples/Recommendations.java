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
package es.saulvargas.ebro.examples;

import es.saulvargas.ebro.Ebro;
import es.saulvargas.ebro.examples.rs.PLSARVF;
import es.saulvargas.ebro.examples.rs.ItemBasedKNNRVF;
import es.saulvargas.ebro.examples.rs.RecommendationVerticesFactory;
import es.saulvargas.ebro.examples.rs.RecommendationVerticesFactory.UserVertex;
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
 * Example: collaborative filtering algorithms.
 *
 * @author Saúl Vargas (saul@vargassandoval.es)
 */
public class Recommendations {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        String fileIn = "total.data";
        String dirOut = "recsys/";

        Map<String, BiFunction<Ebro<Object[]>, BufferedWriter, RecommendationVerticesFactory<String, String, Object[]>>> rvfMap = new HashMap<>();

        rvfMap.put("ib", (ebro, writer) -> new ItemBasedKNNRVF<>(ebro, writer, 100, 50));
        rvfMap.put("plsa", (ebro, writer) -> new PLSARVF<>(ebro, 50, 20, 100, writer));

        rvfMap.forEach((name, supplier) -> {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(dirOut + name))) {
                final Ebro ebro = new Ebro(5000, false, false);

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
