;; SPDX-License-Identifier: EPL-2.0

(defproject egres-jepsen "0.1.0-SNAPSHOT"
  :description "Jepsen snapshot-isolation tests for Egres over pgwire"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0"
            :comments "Provenance: LICENSE-NOTES.md"}
  :dependencies [[org.clojure/clojure "1.12.5"]
                 [jepsen "0.3.13"]
                 [org.postgresql/postgresql "42.7.11"]]
  :main ^:skip-aot jepsen.egres
  :filespecs [{:type :path :path "LICENSE-NOTES.md"}]
  :jvm-opts ["-Djava.awt.headless=true" "-Xmx4g"])
