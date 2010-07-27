(ns amontillado.core
  (:refer-clojure :excludes [read])
  (:import [java.util UUID]
           [java.io File RandomAccessFile]
           [java.util.concurrent ConcurrentHashMap]
           [clojure.lang ILookup Associative PersistentQueue]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel]
           [java.util.zip CRC32]))

(def ^String dir (or (System/getProperty "cask.dir") "/tmp/amontillado"))

(.mkdirs (File. dir))

(def casks (ref (PersistentQueue/EMPTY)))

(def size 209715200)

(def closeable (agent nil))

(def open-casks 5)

(def ^ConcurrentHashMap key-table (ConcurrentHashMap.))

(defrecord Cask [name file channel])

(defrecord Vtable [file-name pos size crc timestamp])

(defn new-cask []
  (let [file-name (format "%s/%s.cask" dir (UUID/randomUUID))
        file-raf (RandomAccessFile. file-name "rw")
        file-channel (.getChannel file-raf)]
    (Cask. file-name file-raf file-channel)))

(defn aquire []
  (let [cask (dosync
              (let [head (peek @casks)]
                (alter casks pop)
                head))]
    (or cask (new-cask))))

(defn release [cask]
  (if (> size (.length ^RandomAccessFile (:file cask)))
    (dosync
     (alter casks conj cask))
    (send-off closeable conj cask)))

(defn close-casks [casks]
  (Thread/sleep (* 1000 30))
  (doseq [cask casks]
    (.close ^FileChannel (:channel cask))
    (.close ^RandomAccessFile (:file cask))))

(defn write [^ByteBuffer bytebuffer]
  (let [crc (.getValue
             (doto (CRC32.)
               (.update (.array bytebuffer))))
        cask (aquire)
        ^FileChannel fc (:channel cask)
        start (.position fc)]
    (.position fc (+ start (.capacity bytebuffer)))
    (release cask)
    (.putLong bytebuffer 8 crc)
    (.write fc bytebuffer (long start))
    (send-off closeable close-casks)
    (Vtable. (:name cask) start (.capacity bytebuffer) crc (.getLong bytebuffer 0))))

(defn read-fn [vtable]
  (with-open [cask (RandomAccessFile. ^String (:file-name vtable) "rw")
              fc (.getChannel cask)]
    (let [bb (ByteBuffer/allocate (:size vtable))]
      (.read fc bb (:pos vtable))
      (.position bb 0)
      (let [time (.getLong bb)
            crc1 (.getLong bb)
            value-size (.getLong bb)
            key-size (.getLong bb)
            value (byte-array value-size)
            _ (.putLong bb 8 0)
            crc (.getValue
                 (doto (CRC32.)
                   (.update (.array bb))))]
        (assert (= crc (:crc vtable)))
        (.position bb 32)
        (.get bb value)
        value))))

(def cask
     (reify
      ILookup
      (valAt [this key] (.valAt this key nil))
      (valAt [this key default]
             (let [key (String. ^bytes key "utf8")]
               (if (.get key-table key)
                 (try
                   (read-fn (.get key-table key))
                   (catch Exception e
                     (throw (Exception. (str "problem reading " key) e))))
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
              entry (doto (ByteBuffer/allocate (long (+ 8 8 8 value-size 8 key-size)))
                      (.putLong 0 time)
                      (.putLong 8 0)
                      (.putLong 16 value-size)
                      (.putLong 24 key-size)
                      (.position 32)
                      (.put ^bytes value)
                      (.put ^bytes key)
                      .flip)]
          (.put key-table (String. ^bytes key "utf8") (write entry)))
        this)))

(defn entry-seq [file]
  (with-open [dis (-> file (RandomAccessFile. "r"))]
    (letfn [(f
             [place]
             (lazy-seq
              (when (> (.length file) place)
                (.seek dis place)
                (let [time (.readLong dis)
                      crc (.readLong dis)
                      value-size (.readLong dis)
                      key-size (.readLong dis)
                      key-bytes (byte-array key-size)]
                  (.seek dis (+ place 32 value-size))
                  (.read dis key-bytes)
                  (cons
                   [(String. key-bytes "utf8")
                    (Vtable. (str (.getParent file) "/" (.getName file))
                             place
                             (+ 8 8 8 8 value-size key-size)
                             crc
                             time)]
                   (f (.getFilePointer dis)))))))]
      (doall (f 0)))))

(defn entries []
  (mapcat entry-seq (rest (file-seq (File. dir)))))

(defn recover []
  (doseq [[key vtable] (entries)]
    (if-let [c-vtable (get key-table key)]
      (.put key-table key (last (sort-by :timestamp [c-vtable vtable]))) 
      (.put key-table key vtable))))

(defn compact []
  ;; TODO:
  )
