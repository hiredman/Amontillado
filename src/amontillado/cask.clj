(ns amontillado.cask
  "Amontillado provides bitcask inspired storage for programs written
  in Clojure. Bitcask is an append only log like file format for
  storing key-value data made popular because it was/is available as a
  storage backend for Riak. You can read Basho's description here
  http://downloads.basho.com/papers/bitcask-intro.pdf.

  Amontillado persists data to disk, while keeping an index(map) in
  memory to make data retrieval fast.

  The main api entry points are open-bitcask, write-key, read-key, and
  delete-key,"
  (:require [clojure.java.io :as io]
            [clojure.core.protocols :as p]
            [clojure.core.reducers :as r])
  (:import (java.nio ByteBuffer)
           (java.util.zip CRC32)
           (java.io RandomAccessFile
                    File)
           (java.nio.channels FileChannel)
           (java.util.concurrent ConcurrentHashMap)))

(defn crc
  "calculate the crc of a byte buffer"
  [^ByteBuffer bb]
  (let [crc (CRC32.)]
    (dotimes [i (.capacity bb)]
      (.update crc (.get bb i)))
    (.getValue crc)))

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

(defn ^ByteBuffer file-record
  "given a key (bytes) and value (bytes) generate a ByteBuffer for being written
   to a file. file-record format is:

  crc -- crc of the rest of the record, calculated on the entire
  record with the crc field as zeros, 8 bytes, a long

  timestamp -- nanoseconds, long

  key size -- byte count in key, long

  value size -- byte count in value, long

  key -- sized as in key size

  value -- sized as in value size"
  [key value]
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
        now (System/nanoTime)
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
  "a key dir entry is what is kept in memory for each item a long with
  the key, it has information for finding the value of a given key on
  disk. it is an array of 4 longs"
  [^long file-id ^long value-size ^long value-position ^long tstamp]
  (let [la (long-array 4)]
    (aset la 0 file-id)
    (aset la 1 value-size)
    (aset la 2 value-position)
    (aset la 3 tstamp)
    la))

(defn allocate
  "given a file channel, lock on the channel them bump the channel's
  position by n

  used to pre-allocate space to write in files"
  [^FileChannel fc ^long n]
  (locking fc
    (let [p (.position fc)]
      (.position fc (+ p n))
      p)))

(defrecord CaskFile [id channel raf])

(defn write-to-cask
  "write a given key and value to a given file"
  [cask-file key value]
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

(defn write-tombstone-to-cask
  "write a tombstone entry for a given key to a given file

  a tombstone for a key is a marker that a given key has been
  `deleted`"
  [cask-file key]
  (let [^FileChannel fc (:channel cask-file)
        file-id (:id cask-file)
        r (file-tombstone-record key)
        pos (allocate fc (.capacity r))]
    (.write fc r pos)
    (.force fc true)
    nil))

(defrecord CaskFiles [files directory limit])

(defn new-cask-files [directory limit]
  (->CaskFiles [] directory limit))

(defn current-file
  "given a CaskFiles returns the current file (the latest one)"
  [cf]
  (let [f (:files cf)]
    (nth f (dec (count f)))))

(defn ^File id-to-file [directory id]
  (io/file directory (.replace (format "%16s" (Long/toHexString id)) " " "0")))

(defn cask-file
  "given a file-id and a directory, returns a CaskFile with that id"
  [file-id directory]
  (let [raf (RandomAccessFile. (id-to-file directory file-id) "rw")]
    (->CaskFile file-id (.getChannel raf) raf)))

(defn check-current-file
  "if the latest file is to large, push a new file"
  [cf]
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
    (doseq [{:keys [^FileChannel channel
                    ^RandomAccessFile raf]} (:files @files)]
      (.close channel)
      (.close raf))))

(defn new-bitcask
  "start a new bitcask with the given limit"
  [directory & [limit]]
  (let [directory (io/file directory)]
    (.mkdirs directory)
    (assert (.exists directory))
    (assert (.isDirectory directory))
    (assert (empty? (.listFiles directory)))
    (->BitCask (ConcurrentHashMap.)
               (atom (new-cask-files directory (or limit (* 1024 1024 200)))))))

(defn write-key
  "given a key, value, and bitcask, and an entry to the bitcask
  mapping the key to the value"
  [^BitCask bc key value]
  (let [cask-files (.-files bc)
        _ (swap! cask-files check-current-file)
        cf (current-file @cask-files)
        kr (write-to-cask cf key value)
        _ (.put ^java.util.Map (.-dict bc) (ByteBuffer/wrap key) kr)]
    nil))

(defn delete-key
  "removes the given key (byte array) from the in memory map and
  writes a tombstone to disk"
  [^BitCask bc key]
  (let [cask-files (.-files bc)
        _ (swap! cask-files check-current-file)
        cf (current-file @cask-files)
        kr (write-tombstone-to-cask cf key)
        _ (.remove ^java.util.Map (.-dict bc) (ByteBuffer/wrap key))]
    nil))

;; TODO: maybe check crc
(defn read-key
  "read the byte (array) value for the given key, returns nil if it
  cannot find a value"
  [^BitCask bc key]
  (let [key (ByteBuffer/wrap key)]
    (when-let [^longs kr (get (.-dict bc) key)]
      (let [file-id (aget kr 0)
            bb (ByteBuffer/allocate (aget kr 1))
            pos (+ (aget kr 2) 32 (.capacity key))
            file (nth (:files @(.-files bc)) file-id)
            ^FileChannel fc (:channel file)]
        (let [bytes-read (.read fc bb pos)]
          (assert (= bytes-read (.capacity bb))))
        (.array bb)))))

(defn read-key-as-bb
  [^BitCask bc key]
  (let [key (ByteBuffer/wrap key)]
    (when-let [^longs kr (get (.-dict bc) key)]
      (let [file-id (aget kr 0)
            file (nth (:files @(.-files bc)) file-id)
            pos (+ (aget kr 2) 32 (.capacity key))
            ^FileChannel fc (:channel file)
            bb (.map fc java.nio.channels.FileChannel$MapMode/READ_ONLY
                     pos (aget kr 1))]
        bb))))

(defn entry-source [cask-file]
  (reify
    p/CollReduce
    (coll-reduce [this f1]
      (p/coll-reduce this f1 (f1)))
    (coll-reduce [_ f1 init]
      (let [^RandomAccessFile c (:raf cask-file)]
        (loop [init init
               offset 0]
          (if (>= offset (.length c))
            init
            (do
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
                    (recur
                     (f1 init
                         (ByteBuffer/wrap k)
                         (key-dir-entry (:id cask-file) vs offset ts))
                     (+ offset 32 ks vs)))
                  (let [ts (.readLong c)
                        crc (.readLong c)
                        ks (.readLong c)
                        k (byte-array ks)
                        _ (.read c k)]
                    (recur
                     (f1 init (ByteBuffer/wrap k) ::tombstone)
                     (+ offset ks (* 8 5)))))))))))))

(defn renumber-files
  "if dead, no longer needed, files are deleted, the existing files
  will need to be renumbered, this function does that"
  [directory files]
  (doall
   (map-indexed
    (fn [i {:keys [id ^FileChannel channel ^RandomAccessFile raf] :as t}]
      (if (= i id)
        t
        (let [from-file (id-to-file directory id)
              to-file (id-to-file directory i)]
          (.close channel)
          (.close raf)
          (assert (-> from-file (.renameTo to-file)))
          (let [raf (RandomAccessFile. to-file "rw")]
            (->CaskFile i (.getChannel raf) raf)))))
    files)))

(defn dead-files
  "given a bitcask return a seq of files that are not referenced by the
  in memory map"
  [^BitCask bc]
  (let [live-files (set (for [^longs v (vals (.-dict bc))] (aget v 0)))]
    (for [f (:files @(.-files bc))
          :when (not (contains? live-files (:id f)))]
      (id-to-file (:directory @(.-files bc)) (:id f)))))

(declare hint-file-last-modified
         load-hint-file)

(defn open-bitcask
  "open a new or existing bitcask

  takes the directory to write the cask files to, and a limit in bytes
  to split the files at

  when opening an already existing bitcask deletes all the dead files"
  [directory & [limit]]
  (let [directory (io/file directory)]
    (if (empty? (.listFiles directory))
      (new-bitcask directory limit)
      (let [files' (for [^File f (file-seq directory)
                         :when (not (.isDirectory f))
                         :when (not (.startsWith (.getName f) "."))]
                     f)
            files (for [^File f files'
                        :let [raf (RandomAccessFile. f "rw")]]
                    (->CaskFile
                     (Long/parseLong (.getName f) 16)
                     (.getChannel raf)
                     raf))
            files (vec (renumber-files directory (sort-by :id files)))]

        (if (and (.exists (io/file directory ".hint"))
                 (> (hint-file-last-modified (io/file directory ".hint"))
                    (apply max (for [f files'] (.lastModified f)))))
          (let [m (load-hint-file (io/file directory ".hint"))
                bc (->BitCask m
                              (atom (assoc (new-cask-files
                                            directory
                                            (or limit (* 1024 1024 200)))
                                      :files files)))]
            (doseq [^File f (dead-files bc)]
              (.delete f))
            bc)
          (let [dict (reduce
                      (fn [^java.util.Map m k v]
                        (if (= v ::tombstone)
                          (do
                            (.remove m k)
                            m)
                          (do
                            (.put m k v)
                            m)))
                      (ConcurrentHashMap.)
                      (r/mapcat entry-source files))
                bc (->BitCask dict
                              (atom (assoc (new-cask-files
                                            directory
                                            (or limit (* 1024 1024 200)))
                                      :files files)))]
            (doseq [^File f (dead-files bc)]
              (.delete f))
            bc))))))

(defn cask-keys
  "return the keys of this bitcask"
  [^BitCask bc]
  (map #(.array ^ByteBuffer %) (keys (.dict bc))))

(defn rollover
  "finds files that contain less then threshold number live keys and
  rolls them over in to new files, hopefully consolidating live files
  so dead files can be deleted"
  [^BitCask bc threshold]
  (let [cf (current-file @(.-files bc))
        freqs (frequencies (for [v (vals (.-dict bc))] (:id v)))
        ids-to-roll (set (for [[file-id freq] freqs
                               :when (> threshold freq)
                               :when (not= file-id (:id cf))]
                           file-id))
        keys-to-roll (for [[^ByteBuffer k ^longs v] (.-dict bc)
                           :when (contains? ids-to-roll (aget v 0))]
                       (.array k))]
    (doseq [key keys-to-roll
            :let [v (read-key bc key)]]
      (write-key bc key v))))

(defn cask-contains?
  "returns true if the given cask has had a value for the given key,
  and that key was not removed after it was written.

  This function does not require a disk seek/read like read-key does."
  [^BitCask bc ^bytes key]
  (contains? (.-dict bc) (ByteBuffer/wrap key)))

(defn cask-density
  "returns a map of file ids to live entry counts"
  [^BitCask bc]
  (apply merge-with +
         (frequencies (map #(aget ^longs % 0) (vals (.-dict bc))))
         (for [f (:files @(.-files bc))]
           {(:id f) 0})))

(defn generate-hint-file
  [^BitCask bc]
  (let [hint-file (io/file (.-directory ^CaskFiles (deref (.-files bc))) ".hint")
        _ (.createNewFile hint-file)
        now (System/currentTimeMillis)
        bb (ByteBuffer/allocate 8)]
    (with-open [f (java.io.FileOutputStream. hint-file)
                c (.getChannel f)]
      (.write c bb)
      (.force c true)
      (.flip bb)
      (doseq [[^ByteBuffer k ^longs v] (.-dict bc)]
        (let [bb (ByteBuffer/allocate
                  (+ 8 (.capacity k) (* 8 4)))]
          (doto bb
            (.putLong (long (.capacity k)))
            (.put (.slice k))
            (.putLong (aget v 0))
            (.putLong (aget v 1))
            (.putLong (aget v 2))
            (.putLong (aget v 3))
            (.flip))
          (while (.hasRemaining bb)
            (.write c bb))))
      (.position c 0)
      (.flip (.putLong bb now))
      (while (.hasRemaining bb)
        (.write c bb)))))

(defn load-hint-file
  [hint-file]
  {:post [(not (empty? %))]}
  (let [m (ConcurrentHashMap.)]
    (with-open [raf (RandomAccessFile. hint-file "r")]
      (.readLong raf)
      (try
        (loop []
          (let [key-size (.readLong raf)
                key (byte-array key-size)
                _ (.readFully raf key)
                a (.readLong raf)
                a' (.readLong raf)
                a'' (.readLong raf)
                a''' (.readLong raf)
                e (key-dir-entry a a' a'' a''')]
            (.put m (ByteBuffer/wrap key) e))
          (recur))
        (catch java.io.EOFException _)))
    m))

(defn hint-file-last-modified
  [hint-file]
  (with-open [raf (RandomAccessFile. hint-file "r")]
    (.readLong raf)))

(defn hint-file-exists?
  [^BitCask bc]
  (.exists (io/file (.-directory ^CaskFiles (deref (.-files bc))) ".hint")))

(defn validate-hint-file
  [^BitCask bc]
  (let [m (load-hint-file
           (io/file (.-directory ^CaskFiles @(.-files bc)) ".hint"))]
    (assert (= (count m) (count (.-dict bc)))
            (list (count m) (count (.-dict bc))))
    (doseq [[k v] (.-dict bc)]
      (assert (= (vec (.get m k)) (vec v))
              (list (vec (.get m k)) (vec v))))))
