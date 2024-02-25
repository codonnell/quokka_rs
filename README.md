# Quokka Search

Quokka Search aims to be a fast and lightweight relational search database. It sacrifices many of the niceties of a typical OLTP database (user-level transactions, foreign key constraints, uniqueness constraints) in order to speed up searches. The sweet spot for Quokka Search is something like a product search page. A typical solution to back this kind of feature is to denormalize data and use a document search database like ElasticSearch. However, denormalization can be painful and restrictive, and ElasticSearch can be complex and expensive to operate. Quokka Search allows the use of a similar relational model and aims to support considerable throughput on a single node.

## Getting Started

Quokka Search exposes an [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) server and does not currently support authentication. To run it, you can compile and run with `cargo run`.

You can connect to the running server using the following JDBC URL: `jdbc:arrow-flight-sql://127.0.0.1:50051`. Please make sure the [Arrow Flight SQL JDBC Driver](https://mvnrepository.com/artifact/org.apache.arrow/flight-sql-jdbc-driver) is on the classpath. You must set a username and password, though they can be anything, and you must set the JDBC parameter `useEncryption` to false.
