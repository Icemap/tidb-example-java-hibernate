package com.pingcap;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Random;
import java.util.function.Function;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.JDBCException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;

/**
 * Main class for the basic Hibernate example.
 * this code based on https://github.com/cockroachlabs/example-app-java-hibernate/
 */
public class ExampleDataSource implements Serializable {

    private static final Random RAND = new Random();

    // Account is our model, which corresponds to the "accounts" database table.
    @Entity
    @Table(name = "accounts")
    public static class Account {

        @Id
        @Column(name = "id")
        public long id;

        public long getId() {
            return id;
        }

        @Column(name = "balance")
        public BigDecimal balance;

        public BigDecimal getBalance() {
            return balance;
        }

        public void setBalance(BigDecimal newBalance) {
            this.balance = newBalance;
        }

        // Convenience constructor.
        public Account(int id, int balance) {
            this.id = id;
            this.balance = BigDecimal.valueOf(balance);
        }

        // Hibernate needs a default (no-arg) constructor to create model objects.
        public Account() {
        }
    }

    private static Function<Session, BigDecimal> addAccounts() throws JDBCException {
        return session -> {
            session.save(new Account(1, 1000));
            session.save(new Account(2, 250));
            session.save(new Account(3, 314159));
            BigDecimal rv = BigDecimal.valueOf(1);
            System.out.printf("APP: addAccounts() --> %.2f\n", rv);
            return rv;
        };
    }

    private static Function<Session, BigDecimal> transferFunds(long fromId, long toId, BigDecimal amount) throws JDBCException {
        return session -> {
            BigDecimal rv = new BigDecimal(0);
            Account fromAccount = session.get(Account.class, fromId);
            Account toAccount = session.get(Account.class, toId);
            if (!(amount.compareTo(fromAccount.getBalance()) > 0)) {
                fromAccount.balance = fromAccount.balance.subtract(amount);
                toAccount.balance = toAccount.balance.add(amount);
                session.save(fromAccount);
                session.save(toAccount);
                rv = amount;
                System.out.printf("APP: transferFunds(%d, %d, %.2f) --> %.2f\n", fromId, toId, amount, rv);
            }
            return rv;
        };
    }

    private static Function<Session, BigDecimal> getAccountBalance(long id) throws JDBCException {
        return s -> {
            BigDecimal balance;
            Account account = s.get(Account.class, id);
            balance = account.getBalance();
            System.out.printf("APP: getAccountBalance(%d) --> %.2f\n", id, balance);
            return balance;
        };
    }

    // Run SQL code in a way that automatically handles the
    // transaction retry logic so we don't have to duplicate it in
    // various places.
    private static BigDecimal runTransaction(Session session, Function<Session, BigDecimal> fn) {
        BigDecimal rv = new BigDecimal(0);
        int attemptCount = 0;

        Transaction txn = session.beginTransaction();

        try {
            rv = fn.apply(session);
            if (!rv.equals(-1)) {
                txn.commit();
                System.out.printf("APP: COMMIT;\n");
            }
        } catch (JDBCException e) {
            System.out.printf("APP: ROLLBACK;\n");
            txn.rollback();
            int sleepMillis = (int) (Math.pow(2, attemptCount) * 100) + RAND.nextInt(100);
            System.out.printf("APP: Hit 40001 transaction retry error, sleeping %s milliseconds\n", sleepMillis);

            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException ignored) {
                // no-op
            }
            rv = BigDecimal.valueOf(-1);
        }
        return rv;
    }

    public static void main(String[] args) {
        // Create a SessionFactory based on our hibernate.cfg.xml configuration
        // file, which defines how to connect to the database.
        SessionFactory sessionFactory
                = new Configuration()
                .configure("hibernate.cfg.xml")
                .addAnnotatedClass(Account.class)
                .buildSessionFactory();

        try (Session session = sessionFactory.openSession()) {
            long fromAccountId = 1;
            long toAccountId = 2;
            BigDecimal transferAmount = BigDecimal.valueOf(100);
            BigDecimal errorValue = BigDecimal.valueOf(-1);

            runTransaction(session, addAccounts());
            BigDecimal fromBalance = runTransaction(session, getAccountBalance(fromAccountId));
            BigDecimal toBalance = runTransaction(session, getAccountBalance(toAccountId));
            if (!fromBalance.equals(errorValue) && !toBalance.equals(errorValue)) {
                // Success!
                System.out.printf("APP: getAccountBalance(%d) --> %.2f\n", fromAccountId, fromBalance);
                System.out.printf("APP: getAccountBalance(%d) --> %.2f\n", toAccountId, toBalance);
            }

            // Transfer $100 from account 1 to account 2
            BigDecimal transferResult = runTransaction(session, transferFunds(fromAccountId, toAccountId, transferAmount));
            if (!transferResult.equals(errorValue)) {
                // Success!
                System.out.printf("APP: transferFunds(%d, %d, %.2f) --> %.2f \n", fromAccountId, toAccountId, transferAmount, transferResult);

                BigDecimal fromBalanceAfter = runTransaction(session, getAccountBalance(fromAccountId));
                BigDecimal toBalanceAfter = runTransaction(session, getAccountBalance(toAccountId));
                if (!fromBalanceAfter.equals(errorValue) && !toBalanceAfter.equals(errorValue)) {
                    // Success!
                    System.out.printf("APP: getAccountBalance(%d) --> %.2f\n", fromAccountId, fromBalanceAfter);
                    System.out.printf("APP: getAccountBalance(%d) --> %.2f\n", toAccountId, toBalanceAfter);
                }
            }
        } finally {
            sessionFactory.close();
        }
    }
}
