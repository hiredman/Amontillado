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
      (.flip bb)
      (let [time (.getLong bb)
            crc (.getLong bb)
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
                 (read-fn (.get key-table key))
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

(defn recover []
  ;; TODO:
  )

(defn compact []
  ;; TODO:
  )
