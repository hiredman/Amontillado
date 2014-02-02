(ns amontillado.cask-test
  (:require [amontillado.cask :refer :all]
            [clojure.test :refer :all])
  (:import (java.io File)
           (java.util UUID)))

(deftest t-cask
  (let [temp (File/createTempFile "foo" "bar")
        _ (.delete temp)]
    (with-open [bc (new-bitcask temp 1024)]
      (write-key bc
                 (.getBytes "foo")
                 (.getBytes "bar"))
      (is (= "bar" (String. (read-key bc (.getBytes "foo"))))))))

(deftest t-delete-key
  (let [temp (File/createTempFile "foo" "bar")
        _ (.delete temp)
        bc (new-bitcask temp 1024)]
    (write-key bc
               (.getBytes "foo")
               (.getBytes "bar"))
    (delete-key bc (.getBytes "foo"))
    (is (= nil (read-key bc (.getBytes "foo"))))
    (let [nbc (open-bitcask temp 1024)]
      (is (= nil (read-key nbc (.getBytes "foo")))))))

(deftest t-crc
  (let [temp (File/createTempFile "foo" "bar")
        _ (.delete temp)
        bc (new-bitcask temp 1024)]
    (with-redefs [crc (constantly 0)]
      (write-key bc
                 (.getBytes "foo")
                 (.getBytes "bar")))
    (is (thrown-with-msg? IllegalStateException
                          #"bad crc"
                          (open-bitcask temp 1024)))))

(deftest t-cask-concurrent
  (let [temp (File/createTempFile "foo" "bar")
        _ (.delete temp)
        bc (new-bitcask temp 1024)
        threads 10
        writes 10
        r (atom {})]
    (doall
     (map deref
          (doall
           (for [_ (range threads)]
             (future
               (dotimes [i writes]
                 (let [k (str (gensym 'k))
                       v (str (gensym 'v))]
                   (write-key bc (.getBytes k) (.getBytes v))
                   (swap! r assoc k v))))))))
    (doseq [[k v] @r]
      (is (= v (String. (read-key bc (.getBytes k))))))))

(deftest t-open-bit-cask
  (let [temp (File/createTempFile "foo" "bar")
        _ (.delete temp)
        bc (new-bitcask temp 1024)
        threads 10
        writes 10
        r (atom {})]
    (doall
     (map deref
          (doall
           (for [_ (range threads)]
             (future
               (dotimes [i writes]
                 (let [k (str (gensym 'k))
                       v (str (gensym 'v))]
                   (write-key bc (.getBytes k) (.getBytes v))
                   (swap! r assoc k v))))))))
    (let [nb (open-bitcask temp 1024)]
      (doseq [[k v] @r]
        (is (= v (String. (read-key nb (.getBytes k)))))))))

(deftest t-cask-keys
  (let [temp (File/createTempFile "foo" "bar")
        _ (.delete temp)
        keys (repeatedly 100 #(str (UUID/randomUUID)))]
    (with-open [bc (new-bitcask temp 1024)]
      (doseq [key keys]
        (write-key bc (.getBytes key) (.getBytes "bar")))
      (is (= (set keys) (set (map #(String. %) (cask-keys bc))))))))
