(ns columnar.bytes
  "Where the bytes come from — the other half of the seam `columnar.source`
  declares.

  `IColumnSource` says what an engine may ask a format for. This says what a
  format may ask the world for, and it is here rather than in a format library
  for the reason that only became visible when there were two of them:
  **`org-apache-parquet` and `org-apache-arrow` need the identical protocol.**
  A copy in each means a caller holding a range-reader cannot hand it to both,
  and an instrument that counts bytes has to be written once per format — two
  things that can substitute for each other, which is one thing.

  ## Why ranges, and not a byte array

  The first version of the Parquet reader took the whole file as a vector. It
  was correct and it defeated the point: these formats exist so that a query
  reads a footer and two column chunks instead of forty gigabytes, and a
  reader that demands the forty gigabytes first has thrown that away before it
  starts.

  So a source answers **ranges**, and every read a format library performs is
  one of them. `counting` records what was asked for, which is how a test
  proves a reader touched one column chunk and nothing else — an assertion on
  the *answer* cannot tell that apart from reading everything and slicing.

  ## Synchronous, deliberately

  `IColumnSource` is synchronous, so this is too. On a host where fetching a
  range is asynchronous — a Worker doing HTTP Range requests — the caller
  pre-fetches into `prefetched` and passes that. Each format exposes what its
  own metadata costs (Parquet's `footer-ranges`, Arrow's tail-then-footer) so
  such a caller knows what to fetch before it can parse anything. Pretending
  otherwise would mean either an async rewrite of the engine or a silent
  whole-file read."
  (:refer-clojure :exclude [bytes]))

(defprotocol IByteSource
  (-size [src] "Total bytes in the object. Known without reading it — from a
    HEAD, a stat, or the catalog's `:object/size-bytes`.")
  (-read-range [src start end] "Bytes in `[start end)` as a vector of unsigned
    ints. `end` is exclusive."))

(defn of-vector
  "A source over a whole file already in memory. For tests, and for objects
  small enough that ranging them is pointless."
  [v]
  (let [v (vec v)]
    (reify IByteSource
      (-size [_] (count v))
      (-read-range [_ start end] (subvec v start end)))))

(defn prefetched
  "A source over ranges someone else already fetched.

  `chunks` is a seq of `[start bytes]`. A range that no chunk covers throws
  rather than returning short: a decoder handed fewer bytes than it asked for
  produces nonsense, and the caller who under-fetched is the one who can fix
  it."
  [size chunks]
  (let [chunks (vec chunks)]
    (reify IByteSource
      (-size [_] size)
      (-read-range [_ start end]
        (or (some (fn [[at bs]]
                    (let [stop (+ at (count bs))]
                      (when (and (<= at start) (<= end stop))
                        (subvec (vec bs) (- start at) (- end at)))))
                  chunks)
            (throw (ex-info "range was not prefetched"
                            {:type :columnar/missing-range
                             :want [start end]
                             :have (mapv (fn [[at bs]] [at (+ at (count bs))]) chunks)})))))))

(defn counting
  "Wrap a source so every range is recorded.

  The reason this exists is the same reason `columnar.source/counting` does:
  correct answers prove nothing about how much was read to get them."
  [src]
  (let [log (atom {:ranges [] :bytes 0})]
    {:log log
     :source (reify IByteSource
               (-size [_] (-size src))
               (-read-range [_ start end]
                 (swap! log (fn [l] (-> l
                                        (update :ranges conj [start end])
                                        (update :bytes + (- end start)))))
                 (-read-range src start end)))}))

(defn read-counts
  "`{:ranges [[start end] ..] :bytes n}`."
  [c]
  @(:log c))

(defn source
  "Coerce `x` to an `IByteSource`. A vector is taken as a whole file."
  [x]
  (if (satisfies? IByteSource x) x (of-vector x)))

(defn of-fn
  "A source of `size` bytes whose ranges come from `(f start end)`.

  The shape a host hands down — `kotobase.lake.reader`'s scan handle carries
  `:size-bytes` and `:read-range` as a number and a function — so this is the
  adapter every format library would otherwise write for itself."
  [size f]
  (reify IByteSource
    (-size [_] size)
    (-read-range [_ start end] (f start end))))
