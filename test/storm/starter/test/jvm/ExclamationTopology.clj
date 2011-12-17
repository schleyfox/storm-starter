(ns storm.starter.test.jvm.ExclamationTopology
  (:use [clojure.test])
  (:import [storm.starter ExclamationTopology])
  (:use [storm.starter.clj.util])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  )

(bootstrap)

(defn- exclamation-invariant
  [input output]
  (is (ms=
        (map (fn [[word]] [(str word "!!!!!!")]) input)
        output)))

(deftest test-exclamation
  (with-quiet-logs
    (with-simulated-time-local-cluster [cluster :supervisors 4]
      (let [ topology (ExclamationTopology/makeTopology)
             results (complete-topology
                       cluster
                       topology
                       :mock-sources {"word" [["happy"] 
                                              ["sad panda"]
                                              ["hooray"]]}
                       :storm-conf {TOPOLOGY-DEBUG true
                                    TOPOLOGY-WORKERS 2}) ]
        (exclamation-invariant [] [])
        (exclamation-invariant
          (read-tuples results "word")
          (read-tuples results "exclaim2"))))))

