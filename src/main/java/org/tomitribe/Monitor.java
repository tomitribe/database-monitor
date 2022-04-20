/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tomitribe;

import org.apache.commons.lang3.StringUtils;
import org.tomitribe.crest.api.Command;
import org.tomitribe.crest.api.Option;
import org.tomitribe.crest.api.StreamingOutput;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.Objects;

/**
 * Small database monitor tool to trace when the failover happens and gives the name of the current node
 */
public class Monitor {

    /**
     * CREST command query the database. It has currently the JDBC driver class hardcoded because it's for our own testing.
     * But it can be passed in as a parameter.
     *
     * @param url The JDBC URL to connect to the SQL database ActiveMQ is hooked up
     * @param user The JDBC username to create a connection
     * @param password The JDBC password associated with the password
     * @return Returns a set of log messages
     * @throws Exception Might get some JDBC exceptions here and there
     */
    @Command("query")
    public StreamingOutput query(final @Option("url") String url,
                                 final @Option("user") String user,
                                 final @Option("password") String password) throws Exception {


        // extract this as parameters if needed
        Class.forName("org.mariadb.jdbc.Driver");
        final String query = "select BROKER_NAME,TIME from ACTIVEMQ_LOCK";

        Lock last = null;
        Lock current = null;

        try (final Connection connection = DriverManager.getConnection(url, user, password);
             final PreparedStatement ps = connection.prepareStatement(query)) {

            while (true) {

                try (final ResultSet resultSet = ps.executeQuery()) {

                    if (!resultSet.next()) {
                        throw new IllegalStateException("Could not read ACTIVEMQ_LOCKS. No result found.");
                    }

                    final String brokerName = resultSet.getString(1);
                    final long brokerTime = resultSet.getLong(2);

                    current = new Lock(brokerName, Instant.ofEpochMilli(brokerTime));

                    // if (last == null || !last.equals(current)) {
                    if (last == null
                        || last.brokerName() == null
                        || !last.brokerName().equals(current.brokerName())) {

                        // the broker has changed.... do something
                        System.out.printf("[%s] Broker changed from %s to %s%n", Instant.now(), last, current);

                        last = current;
                    }

                }

                // don't hammer the database for the moment ....
                Thread.sleep(100);

            }
        }

    }

    public record Lock(String brokerName, Instant time) {

        public Lock {
            StringUtils.isNoneEmpty(brokerName);
            Objects.requireNonNull(time);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {return true;}
            if (o == null || getClass() != o.getClass()) {return false;}
            final Lock lock = (Lock) o;
            return Objects.equals(brokerName, lock.brokerName) && Objects.equals(time, lock.time);
        }

        @Override
        public int hashCode() {
            return Objects.hash(brokerName, time);
        }
    }

    public static void main(final String[] args) throws Exception {
        new Monitor().query("jdbc:mariadb://ec2-3-228-12-181.compute-1.amazonaws.com:3306/activemq", "amq-user", "amq-password");
    }

}
