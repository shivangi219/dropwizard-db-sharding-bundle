# Dropwizard DB Sharding Bundle

Application level sharding for traditional relational databases accessed using Hibernate ORM.

See tests for details and code for documentation.

Apache Licensed

## Usage

This library is available in clojars. Please use the following repository setting:

```
<repository>
    <id>clojars</id>
    <name>Clojars repository</name>
    <url>https://clojars.org/repo</url>
</repository>
```

The project dependencies are:
```
<dependency>
    <groupId>io.dropwizard.sharding</groupId>
    <artifactId>db-sharding-bundle</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```
# Introduction


Dropwizard Hibernate Bundle provides easy way to integrate dropwizard applications with Hibernate ORM to access MySQL, Postgresql, Oracle and many other RDBMS systems. This library helps to scale such applications by providing the ability to scale their operations across multiple database clusters, thereby providing the apps the ability transcend the scalability limitations imposed buy having to work with a single database cluster.

This library is battletested with extensive usage at PhonePe and should be easy to use for most people. For existing applications refactoring will probabbly be necessary to access the full set of features.

## Terms
This library works by providing canned DAOs relevant for popular usecases. However, the simple abstractions provided by the DAO transaparently scales across multiple database clusters. The following nomenclature should help you through the rest of the documentation.

### @LookupKey and LookupDAA

Consider the following normal entity class:

```
@Entity
@Table(name="customers")
public class Customer {
    @Id
    @Column
    public String id;

    @Column
    public String name;

    @OneToMany(mappedBy = "transaction")
    @Cascade(CascadeType.ALL)
    private List<Transaction> transactions;
}

@Entity
@Table(name="transactions")
public class Transaction {
    @Id
    @GeneratedValue
    public long id;

    @ManyToOne
    @JoinColumn(name = "customer")
    public Customer customer;

    @Column
    public long amount;

    @Column
    public boolean cancelled;
}
```

In the above schema there are two entities: Customer and Transaction. Every customer is independent of thers. However all transactions belong to a customer. Let's assume that customers perform taska on their transactions, for example create new transactions, cancel them, see history etc.

To scale this system to handle potentially 10's or 100's of millions of customers, obviously pumping everything to same DB would not work after some time. A quick glance at the usecase shows us that the customers do not share data between each other, that is, every customer is independent of each other. Therefore we choose the following:
* Every customer is a Lookup/Parent/Master object
* Every transaction is related to a customer. So its a Related/Child object.

The sharding strategy will ensure the following:
* Customers will be evenly distributed across mutiple shards
* Transactions for a customer will reside in the same shard as the customer

To achieve the above, the following needs to be done:

- Add @LookupKey annotation to the Customer::id field

```
@Entity
@Table(name="customers")
public class Customer {
    @Id
    @LookupKey
    @Column
    public String id;

    @Column
    public String name;

    @OneToMany(mappedBy = "transaction")
    @Cascade(CascadeType.ALL)
    private List<Transaction> transactions;
}
```
