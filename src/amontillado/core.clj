(ns amontillado.core
  (:refer-clojure :exclude [get])
  (:import [java.util UUID]
           [java.io File RandomAccessFile]
           [java.util.concurrent ConcurrentHashMap]
           [clojure.lang ILookup Associative]))

(def dir (or (System/getProperty "cask.dir") "/tmp/amontillado"))

(def port (or (System/getProperty "cask.port") "7777"))

(def cask-id nil)

(defn cask-file [uuid]
  (let [dir (if (.endsWith dir "/")
              dir
              (str dir "/"))]
    (.getChannel (RandomAccessFile. (File. (str dir uuid))))))

(def cask′ nil)

(defn new-cask-file []
  (locking cask-id
    (locking cask′
      (alter-var-root cask-id #(do % (UUID/randomUUID)))
      (alter-var-root cask′ #(do % (cask-file cask-id))))))

(def keys (ConcurrentHashMap.))

(defn write [bytebuffer]
  (let [[cask cask-id] (locking cask′ [cask′ cask-id])
        start (.size cask)
        size (.capacity bytebuffer)]
    (locking cask
      (.position cask (+ start size)))
    (.write cask bytebuffer start)
    [start size cask-id]))

(defn read [key]
  (let [loc (.get keys key)]
    (with-open [cask (cask-file (clojure.core/get loc 3))]
      (.read cask (clojure.core/get loc 0) (clojure.core/get loc 1)))))

(defn select-value [blob]
  ;; TODO:
  )

(defn byte-buffer [time value-size value key-size key]
  ;; TODO:
  )

(defprotocol Bytes
  (get-bytes [thing]))

(def cask
     (reify
      ILookup
      (valAt [this key] (.valAt this key nil))
      (valAt [this key default]
             (let [key (String. key "utf8")]
               (if (contains? keys key)
                 (select-value (read key))
                 default)))
      Associative
      (containsKey [this key]
                   (let [sent (Object.)]
                     (not (identical? ) (.valAt this key sent))))
      (entryAt [this key]
               [key (.valAt this key)])
      (assoc [this key value]
        (let [key-size (count key)
              value-size (count value)
              time (System/nanoTime)
              entry (byte-buffer time value-size value key-size key)]
          (.put keys (String. key "utf8") entry))
        this)))

;; table pos | len | uuid | val size
;; (.put keys key (.write bytebuffer))
