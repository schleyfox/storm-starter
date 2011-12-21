(ns storm.starter.test.jvm.ExclamationTopology
  (:use [clojure.test])
  (:import [storm.starter ExclamationTopology])
  (:use [storm.test.util])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  )

(bootstrap)

(defn- exclamation-p
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
        (exclamation-p [] [])
        (exclamation-p
          (read-tuples results "word")
          (read-tuples results "exclaim2"))))))

