(defproject jepsen.percona-gr "0.1.0-SNAPSHOT"
  :description "Jepsen Percona Group Replication tests"
  :url "https://github.com/jepsen-io/percona-gr"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.2.7-SNAPSHOT"]
                 [com.github.seancorfield/next.jdbc "1.2.780"]
                 [mysql/mysql-connector-java "8.0.29"]
                 [com.google.guava/guava "30.1-jre"]
                 ]
  :main jepsen.percona-gr
  :jvm-opts ["-Djava.awt.headless=true"]
  :repl-options {:init-ns jepsen.percona-gr}
  :profiles {:dev {:dependencies [[org.clojure/test.check "1.1.1"]]}})
