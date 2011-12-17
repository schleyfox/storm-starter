(ns storm.starter.test.clj.word-count
  (:use [clojure.test])
  (:use [storm.starter.clj.word-count])
  (:use [storm.starter.clj.util])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  )

(bootstrap)

(defn- word-count-invariant
  [input output]
  (is (=
        (frequencies
          (reduce
            (fn [acc sentence]
              (concat acc (.split (first sentence) " ")))
            []
            input))
        (reduce
          (fn [m [word n]]
            (assoc m word n))
          {}
          output))))

(deftest test-word-count
  (with-quiet-logs
    (with-simulated-time-local-cluster [cluster :supervisors 4]
      (let [ topology (mk-topology)
             results (complete-topology 
                       cluster
                       topology
                       :mock-sources {"1" [["little brown dog"]
                                           ["petted the dog"]
                                           ["petted a badger"]]
                                      "2" [["cat jumped over the door"]
                                           ["hello world"]]}
                       :storm-conf {TOPOLOGY-DEBUG true
                                    TOPOLOGY-WORKERS 2}) ]
        ; test initial case
        (word-count-invariant [] [])
        ; test after run
        (word-count-invariant
          (concat (read-tuples results "1") (read-tuples results "2"))
          (read-tuples results "4"))))))
