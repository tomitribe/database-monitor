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
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.jolokia.client.BasicAuthenticator;
import org.jolokia.client.J4pClient;
import org.jolokia.client.exception.J4pException;
import org.jolokia.client.exception.J4pRemoteException;
import org.jolokia.client.request.J4pListRequest;
import org.jolokia.client.request.J4pReadRequest;
import org.jolokia.client.request.J4pResponse;
import org.jolokia.client.request.J4pTargetConfig;
import org.json.simple.JSONObject;
import org.tomitribe.crest.api.Command;
import org.tomitribe.crest.api.Default;
import org.tomitribe.crest.api.Option;
import org.tomitribe.crest.api.StreamingOutput;
import org.tomitribe.util.Duration;
import org.tomitribe.util.TimeUtils;

import javax.management.MalformedObjectNameException;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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
                                 final @Option("password") String password,
                                 final @Option("pid") String pid,
                                 final @Option("jstack-duration") @Default("15 minutes") Duration jstackDuration) throws Exception {

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

                        // We'll now take the jstack of the process to see what's happening
                        // and more precisely, see if some pending threads are holding the process and the lock
                        // don't do the first time though
                        if (last != null) {

                            final long time = jstackDuration.getTime(TimeUnit.NANOSECONDS);
                            final long start = System.nanoTime();

                            while (System.nanoTime() - start < time) {

                                final long remainingNanos = time - System.nanoTime() + start;
                                System.out.printf("-----%n" +
                                                  "Collecting information from ActiveMQ process id %s " +
                                                  "for %s to look for threads holding the shutdown%n" +
                                                  "-----%n",
                                                  pid, TimeUtils.abbreviate(remainingNanos, TimeUnit.NANOSECONDS));

                                jstackIt(pid, jstackDuration, System.out::println);

                                // and now see if we can dump some data from JMX
                                dumpJmx();
                            }
                        }


                        last = current;
                    }

                }

                // don't hammer the database for the moment ....
                Thread.sleep(100);

            }
        }

    }

    /**
     * This creates an external process to call jstack. For simplicity, we consider jstack is in the path.
     * No JAVA_HOME search or pid check. We just through the command right away to the shell (either windows or Unix)
     * @param pid PID of the java process to JStack
     * @throws Exception If something goes wrong
     */
    private void jstackIt(final String pid, final Duration duration, final Consumer<String> out) throws Exception {
        final boolean isWindows = System.getProperty("os.name").toLowerCase().startsWith("windows");

        final ProcessBuilder builder = new ProcessBuilder();
        if (isWindows) {
            builder.command("cmd.exe", "/c", "jstack", pid);
        } else {
            // builder.command("sh", "-c", "jstack", pid);
            builder.command("jstack", pid);
        }
        builder.directory(new File(System.getProperty("user.home")));
        final Process process = builder.start();
        final StreamGobbler streamGobbler = new StreamGobbler(process.getInputStream(), out);
        Executors.newCachedThreadPool().submit(streamGobbler);
        final int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IllegalStateException(
                "JStack process did not returned successfully. Exit code is " + exitCode);
        }

        // don't go too fast
        Thread.sleep(1000);
    }

    private void dumpJmx() throws Exception {
        // final J4pClient client = J4pClient.url("http://ec2-44-201-59-75.compute-1.amazonaws.com:8161/api/jolokia")
        // final J4pClient client = J4pClient.url("http://ec2-3-235-231-230.compute-1.amazonaws.com:8161/api/jolokia")
        final J4pClient client = J4pClient.url("http://127.0.0.1:8161/api/jolokia/")
                                 .user("admin")
                                 .password("admin")
                                 .authenticator(new BasicAuthenticator(){
                                     @Override
                                     public void authenticate(
                                         final HttpClientBuilder pBuilder, final String pUser, final String pPassword) {
                                         pBuilder.setDefaultHeaders(
                                             List.of(new BasicHeader("Origin", "http://localhost")));
                                         super.authenticate(pBuilder, pUser, pPassword);
                                     }
                                 }.preemptive())
                                 .connectionTimeout(3000)
                                 .build();

        list(client, "");
    }

    private void list(final J4pClient client, final String path) throws Exception {
        final J4pResponse<J4pListRequest> list;
        try {
            list = client.execute(new J4pListRequest(path));
        } catch (final J4pRemoteException e) {
            if (!"java.lang.UnsupportedOperationException".equals(e.getErrorType())) {
                System.out.println("Failed to list MBeans for " + path + ". " + e.getMessage());
            }
            return;
        }
        for (Object key : ((JSONObject) list.getValue()).keySet()) {
            final String stringKey = String.valueOf(key);
            if (stringKey.contains("type=")) {
                final String pObjectName = path + ":" + stringKey;

                if (!(
                    pObjectName.startsWith("java.lang")
                    || pObjectName.startsWith("org.apache")
                )) {
                    System.out.println("Skipping MBean " + pObjectName);
                    continue;
                }

                try {
                    final J4pResponse<J4pReadRequest> read = client.execute(new J4pReadRequest(pObjectName));
                    System.out.println(pObjectName + " > " + ((JSONObject) read.getValue()).toJSONString());
                } catch (final J4pRemoteException e) {
                    if (!"java.lang.UnsupportedOperationException".equals(e.getErrorType())) {
                        System.out.println("Failed to read MBeans for " + pObjectName + ". " + e.getMessage());
                    }
                }
            } else {
                try {
                    list(client, "" + stringKey);
                } catch (final J4pRemoteException e) {
                    if (!"java.lang.UnsupportedOperationException".equals(e.getErrorType())) {
                        System.out.println("Failed to list MBeans for " + stringKey + ". " + e.getMessage());
                    }
                }
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

    /**
     * When running a process, we need to at least consume the output of the process will be hanging around.
     * This class simply consumes the InputStream. It does implement Runnable to we can hook it up to an Exceutor
     */
    private static class StreamGobbler implements Runnable {
        private InputStream inputStream;
        private Consumer<String> consumer;

        public StreamGobbler(InputStream inputStream, Consumer<String> consumer) {
            this.inputStream = inputStream;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            new BufferedReader(new InputStreamReader(inputStream)).lines()
                                                                  .forEach(consumer);
        }
    }

    public static void main(final String[] args) throws Exception {
        new Monitor().query("jdbc:mariadb://ec2-3-228-12-181.compute-1.amazonaws.com:3306/activemq",
                            "amq-user",
                            "amq-password",
                           "69114",
                            new Duration("15 seconds"));
    }

    /*
    http://ec2-44-201-59-75.compute-1.amazonaws.com:8161/api/jolokia/read/java.lang:type=*

     */

}
