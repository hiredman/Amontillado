(ns amontillado.core
  (:refer-clojure :excludes [read])
  (:import [java.util UUID]
           [java.io File RandomAccessFile]
           [java.util.concurrent ConcurrentHashMap]
           [clojure.lang ILookup Associative PersistentQueue]
           [java.nio ByteBuffer]))

(def dir (or (System/getProperty "cask.dir") "/tmp/amontillado"))

(.mkdirs (File. dir))

(def casks (ref (PersistentQueue/EMPTY)))

(def size (* 1024 1024 200))

(def closeable (agent nil))

(def open-casks 5)

(defrecord Cask [name file channel])

(defrecord Vtable [file-name pos size])

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
  (if (> size (.length (:file cask)))
    (dosync
     (alter casks conj cask))
    (send-off closeable conj cask)))

(defn close-casks [casks]
  (Thread/sleep (* 1000 30))
  (doseq [cask casks]
    (.close (:channel cask))
    (.close (:file cask))))

(defn write [bytebuffer]
  (let [cask (aquire)
        fc (:channel cask)
        start (.position fc)]
    (try
     (.position fc (+ start (.capacity bytebuffer)))
     (finally
      (release cask)))
    (.write fc bytebuffer start)
    (send-off closeable close-casks)
    (Vtable. (:name cask) start (.capacity bytebuffer))))

(defonce key-table (ConcurrentHashMap.))

(defn read-fn [vtable]
  (with-open [cask (RandomAccessFile. (:file-name vtable) "rw")]
    (.seek cask (:pos vtable))
    (let [time (.readLong cask)
          value-size (.readLong cask)
          value (byte-array value-size)
          key-size (.readLong cask)
          key (byte-array key-size)]
      (.read cask value)
      (.read cask key)
      value)))

(def cask
     (reify
      ILookup
      (valAt [this key] (.valAt this key nil))
      (valAt [this key default]
             (let [key (String. key "utf8")]
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
              entry (doto (ByteBuffer/allocate (+ 8 8 value-size 8 key-size))
                      (.putLong 0 time)
                      (.putLong 8 value-size)
                      (.putLong 16 key-size)
                      (.position 24)
                      (.put value)
                      (.put key)
                      .flip)]
          (.put key-table (String. key "utf8") (write entry)))
        this)))
