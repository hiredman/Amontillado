(ns amontillado.cask
  (:require [clojure.java.io :as io])
  (:import (java.nio ByteBuffer)
           (java.util.zip CRC32)
           (java.io RandomAccessFile
                    File)
           (java.nio.channels FileChannel)))

(defn crc [^ByteBuffer bb]
  (.getValue
   (doto (CRC32.)
     (.update (.array bb)))))

(defn ^ByteBuffer file-record-no-crc [ts ^bytes k ^bytes v]
  (let [bb (ByteBuffer/allocate (+ (/ 64 8) ;; long crc
                                   (/ 64 8) ;; long ts
                                   (/ 64 8) ;; long key size
                                   (/ 64 8) ;; long value size
                                   (count k)
                                   (count v)))]
    (doto bb
      (.putLong 0 0) ;; 0 crc
      (.putLong 8 ts)
      (.putLong 16 (count k))
      (.putLong 24 (count v))
      (.position 32)
      (.put k)
      (.position (+ 32 (count k)))
      (.put v))))

(defn ^ByteBuffer file-record [key value]
  (let [bb (file-record-no-crc (System/nanoTime) key value)
        c (crc bb)]
    (.putLong bb 0 c)
    (.position bb 0)
    bb))

(defn ^ByteBuffer file-tombstone-record [^bytes key]
  (let [key-size (count key)
        bb (ByteBuffer/allocate (+ (/ 64 8) ;; marker
                                   (/ 64 8) ;; marker
                                   (/ 64 8) ;; long crc
                                   (/ 64 8) ;; long ts
                                   (/ 64 8) ;; long key size
                                   key-size))
        now (System/currentTimeMillis)
        _ (doto bb
            (.putLong 0 Long/MAX_VALUE)
            (.putLong 8 Long/MAX_VALUE)
            (.putLong 16 0) ;; 0 crc
            (.putLong 24 now) ;; ts
            (.putLong 32 key-size) ;; ts
            (.position 40) ;;
            (.put key))
        c (crc bb)]
    (.putLong bb 16 c)
    (.position bb 0)
    bb))

(defn key-dir-entry
  [^long file-id ^long value-size ^long value-position ^long tstamp]
  (let [la (long-array 4)]
    (aset la 0 file-id)
    (aset la 1 value-size)
    (aset la 2 value-position)
    (aset la 3 tstamp)
    la))

(defn allocate [^FileChannel fc ^long n]
  (locking fc
    (let [p (.position fc)]
      (.position fc (+ p n))
      p)))

(defrecord CaskFile [id channel raf])

(defn write-to-cask [cask-file key value]
  (let [^FileChannel fc (:channel cask-file)
        file-id (:id cask-file)
        r (file-record key value)
        value-pos (allocate fc (.capacity r))]
    ;; TODO: make sure entire buffer is written
    (let [x (.write fc r value-pos)]
      (assert (= x (.capacity r))))
    (.force fc true)
    ;; TODO: real timestamp
    (key-dir-entry file-id (count value) value-pos 0)))

(defn write-tombstone-to-cask [cask-file key]
  (let [^FileChannel fc (:channel cask-file)
        file-id (:id cask-file)
        r (file-tombstone-record key)]
    (while (not (zero? (.remaining r)))
      (.write fc r))
    (.force fc true)
    nil))

(defrecord CaskFiles [files directory limit])

(defn new-cask-files [directory limit]
  (->CaskFiles [] directory limit))

(defn current-file [cf]
  (let [f (:files cf)]
    (nth f (dec (count f)))))

(defn cask-file [index directory]
  (let [raf (RandomAccessFile.
             (io/file directory
                      (.replace (format "%32s"
                                        (Long/toHexString index))
                                " "
                                "0")) "rw")]
    (->CaskFile index (.getChannel raf) raf)))

(defn check-current-file [cf]
  {:pre [(:files cf) (:directory cf) (:limit cf)]}
  (if (empty? (:files cf))
    (->CaskFiles [(cask-file 0 (:directory cf))]
                 (:directory cf)
                 (:limit cf))
    (let [cff (current-file cf)]
      (if (> (.size ^FileChannel (:channel cff)) (:limit cf))
        (let [files (:files cf)
              new-cf (cask-file (count files) (:directory cf))]
          (->CaskFiles (conj files new-cf)
                       (:directory cf)
                       (:limit cf)))
        cf))))

(deftype BitCask [dict files]
  java.io.Closeable
  (close [_]
    (doseq [{:keys [^FileChannel channel ^RandomAccessFile raf]} (:files @files)]
      (.close channel)
      (.close raf))))

(defn new-bitcask [directory & [limit]]
  (let [directory (io/file directory)]
    (assert (.mkdirs directory))
    (->BitCask (atom {})
               (atom (new-cask-files directory (or limit (* 1024 1024 200)))))))

(defn write-key [^BitCask bc key value]
  (let [cask-files (.-files bc)
        _ (swap! cask-files check-current-file)
        cf (current-file @cask-files)
        kr (write-to-cask cf key value)
        _ (swap! (.-dict bc) assoc (ByteBuffer/wrap key) kr)]
    nil))

(defn delete-key [^BitCask bc key]
  (let [cask-files (.-files bc)
        _ (swap! cask-files check-current-file)
        cf (current-file @cask-files)
        kr (write-tombstone-to-cask cf key)
        _ (swap! (.-dict bc) dissoc (ByteBuffer/wrap key))]
    nil))

;; TODO: check crc
(defn read-key [^BitCask bc key]
  (let [key (ByteBuffer/wrap key)]
    (when-let [^longs kr (get @(.-dict bc) key)]
      (let [file-id (aget kr 0)
            bb (ByteBuffer/allocate (aget kr 1))
            pos (+ (aget kr 2) 32 (.capacity key))
            file (nth (:files @(.-files bc)) file-id)
            ^FileChannel fc (:channel file)]
        ;; TODO: this should not lock
        (locking fc
          (.position fc pos)
          (while (not (zero? (.remaining bb)))
            (.read fc bb)))
        (.array bb)))))

(defn entry-seq [cask-file]
  (let [^RandomAccessFile c (:raf cask-file)]
    ((fn continue [offset]
       (when (< offset (.length c))
         (lazy-seq
          (.seek c offset)
          (let [crc (.readLong c)
                ts (.readLong c)]
            (if-not (and (= crc Long/MAX_VALUE)
                         (= ts Long/MAX_VALUE))
              (let [ks (.readLong c)
                    vs (.readLong c)
                    k (byte-array ks)
                    _ (.read c k)
                    v (byte-array vs)
                    _ (.read c v)
                    bb (file-record-no-crc ts k v)]
                (when-not (= (amontillado.cask/crc bb) crc)
                  (throw (IllegalStateException.
                          (str "bad crc in " c " at " offset))))
                (cons
                 [(ByteBuffer/wrap k)
                  (key-dir-entry (:id cask-file)
                                 vs
                                 offset
                                 ts)]
                 (continue (+ offset 32 ks vs))))
              (let [ts (.readLong c)
                    crc (.readLong c)
                    ks (.readLong c)
                    k (byte-array ks)
                    _ (.read c k)]
                (cons
                 [(ByteBuffer/wrap k) ::tombstone]
                 (continue (+ offset ks (* 8 5))))))))))
     0)))

(defn open-bitcask [directory & [limit]]
  (let [directory (io/file directory)]
    (if (empty? (.listFiles directory))
      (new-bitcask directory limit)
      ;; TODO: renumber files
      (let [files (for [^File f (file-seq directory)
                        :when (not (.isDirectory f))
                        :let [raf (RandomAccessFile. f "rw")]]
                    (->CaskFile
                     (Long/parseLong (.getName f) 16)
                     (.getChannel raf)
                     raf))
            files (vec (sort-by :id files))
            dict (into {} (mapcat entry-seq files))
            dict (into {} (for [[k v] dict
                                :when (not= v ::tombstone)]
                            [k v]))]
        (->BitCask (atom dict)
                   (atom (assoc (new-cask-files directory
                                                (or limit (* 1024 1024 200)))
                           :files files)))))))
