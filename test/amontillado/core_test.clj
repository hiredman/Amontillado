(ns amontillado.core-test
  (:use [amontillado.core] :reload-all)
  (:use [clojure.test])
  (:import [java.util UUID]))

(deftest simple-read-write
  (dotimes [n 10000]
    (let [w (.getBytes (str n))]
      (assoc cask w w)))
  (doseq [k (keys key-table)]
    (= k (String. (get cask (.getBytes k)) "utf8"))))

(deftest concurrent-read-write
  (.clear key-table)
  (let [agents (take 20 (repeatedly #(agent nil)))]
    (doseq [agent agents]
      (send-off agent
                (fn [_]
                  (dotimes [ii 200]
                    (let [a (.getBytes (str (rand-int ii) ii))]
                      (assoc cask a a))))))
    (apply await agents)
    (doseq [k (keys key-table)]
      (= k (String. (get cask (.getBytes k)) "utf8")))))
