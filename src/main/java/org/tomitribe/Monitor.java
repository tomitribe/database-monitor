package org.tomitribe;

import org.tomitribe.crest.api.Command;
import org.tomitribe.crest.api.Option;
import org.tomitribe.crest.api.StreamingOutput;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Monitor {

    @Command("query")
    public StreamingOutput query(final @Option("url") String url,
                                 final @Option("user") String user,
                                 final @Option("password") String password) throws Exception {


        Class.forName("org.mariadb.jdbc.Driver");
        final String query = "select BROKER_NAME from ACTIVEMQ_LOCK";

        String lastMasterBroker;
        String masterBroker;

        while (true) {
            try (final Connection connection = DriverManager.getConnection(url, user, password);
                 final PreparedStatement ps = connection.prepareStatement(query);
                 final ResultSet resultSet = ps.executeQuery()) {

                while (resultSet.next()) {
                    masterBroker = resultSet.getString(1);
                }

                if (!lastMasterBroker.equals(masterBroker)) {
                    // the broker has changed.... do something


                    lastMasterBroker = masterBroker;
                }

            }
        }



    }



}
