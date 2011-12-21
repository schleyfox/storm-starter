(ns storm.starter.test.jvm.RollingTopWords
  (:use [clojure.test])
  (:import [storm.starter RollingTopWords])
  (:use [storm.test.util])
  (:use [storm.test.capturing-topology])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  )

(bootstrap)

; This test ensures that objects are getting rolled out over time.
; This checks the counts as they come off the count bolt.
(deftest test-rolling-count-objects
  (with-quiet-logs
    (with-simulated-time-local-cluster [cluster]
      (with-capturing-topology [ capture
                                 cluster
                                 (RollingTopWords/makeTopology)
                                 :mock-sources ["word"]
                                 :storm-conf {TOPOLOGY-DEBUG true} ]
        (feed-spout! capture "word" ["the"])
        (feed-spout! capture "word" ["the"])
        (feed-spout! capture "word" ["the"])
        (feed-spout-and-wait! capture "word" ["the"])
        (is (= ["the" 4]
               (last (read-current-tuples capture "count"))))
        ; let's bust a minute ahead
        (advance-cluster-time cluster 50 9)
        (feed-spout! capture "word" ["the"])
        (feed-spout-and-wait! capture "word" ["the"])
        (is (= ["the" 6]
               (last (read-current-tuples capture "count"))))
        ; let's bust into the next 10 minutes
        (advance-cluster-time cluster 540 9)
        (feed-spout-and-wait! capture "word" ["the"])
        (is (= ["the" 3]
               (last (read-current-tuples capture "count"))))

      ))
  )
)

(deftest test-rolling-top-words
  (with-quiet-logs
    (with-simulated-time-local-cluster [cluster]
      (with-capturing-topology [ capture
                                 cluster
                                 (RollingTopWords/makeTopology)
                                 :mock-sources ["word"]
                                 :storm-conf {TOPOLOGY-DEBUG true} ]
        (let [ words ["the" "bird" "is" "a" "word"] ]
          ; pump some data in to get these rankings set up
          (doall
            (map-indexed
              (fn [i word]
                (dotimes [j (/ 20 (+ i 1))]
                  (feed-spout! capture "word" [word])))
              words))
          ; this will give us at least 10 seconds of pause,
          ; we only need 2
          (wait-for-capture capture)
          (doseq [word words]
            ; this pass is to shake out all the rankings
            ; we get 10 simulated seconds between iterations so
            ; life is good
            (feed-spout-and-wait! capture "word" [word]))
          ; now we should have good results
          (is (= (map-indexed (fn [i word] [word (int (+ (/ 20 (+ i 1)) 1))])
                              (take 3 words))
                 (first (last (read-current-tuples capture "merge"))))))))
  )
)
