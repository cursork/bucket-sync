(ns bucket-sync.core
  "Very simple tool for syncing between two s3 buckets, either of which may be
  in an isolated region (e.g. China). Run as:

      lein run -- config/foo.clj

  See config/example.clj for valid options."
  (:require [clojure.core.async        :refer [<! <!! chan onto-chan go go-loop close!]]
            [clojure.data.codec.base64 :as b64]
            [clojure.java.io           :as io]
            [amazonica.aws.s3          :as s3]
            [taoensso.timbre           :as timbre])
  (:import java.security.MessageDigest
           com.amazonaws.services.s3.model.AmazonS3Exception))

(defn pr-ch
  "Debug fn. Blocking take everything off a channel and prn it as it arrives."
  [ch]
  (loop []
    (when-let [x (<!! ch)]
      (prn x)
      (recur))))

(defn synchro-prn
  "Synchronise printing on calls to this fn."
  [& args]
  (locking synchro-prn
    (apply prn args)))

;; The MD5 sum is defined to be encoded as B64. So we just go straight to
;; MessageDigest and deal with bytes rather than use (for example) clj-digest
(defn md5-b64
  [bs]
  (let [md (MessageDigest/getInstance "MD5")]
    (.update md bs)
    (-> (.digest md)
        b64/encode
        (String.))))

(defn stream-object
  [cfg from-p from-bucket from-key to-p to-bucket to-key]
  (let [obj      (s3/get-object from-p {:bucket-name from-bucket :key from-key})
        metadata (:object-metadata obj)
        stream   (:object-content obj)]
    (s3/put-object to-p {:bucket-name  to-bucket
                         :key          to-key
                         :input-stream stream
                         :metadata     metadata})))

(defn valid-aws-creds?
  [{:keys [profile endpoint]}]
  (and (string? profile) (string? endpoint)))

(defn bucket-exists?
  [p bucket]
  (try
    (s3/get-bucket-location p {:bucket-name bucket})
    true
    (catch AmazonS3Exception _
      nil)))

(defn all-bucket-keys
  "Returns a channel which will be fed all pairs of keys and etags in the given
  bucket matching the prefix."
  [p bucket prefix]
  (let [ch (chan 500)]
    (go-loop [marker nil]
      (let [resp (s3/list-objects p {:bucket-name bucket
                                     :marker      marker
                                     :prefix      prefix
                                     :max-keys    100})]
        (onto-chan ch
                   (->> resp
                        :object-summaries
                        (map (juxt :key :etag)))
                   false)
        ;(synchro-prn resp)
        (if (:truncated? resp)
          (recur (:next-marker resp))
          (close! ch))))
    ch))

(defn exists-etag-matches?
  [p bucket k etag]
  (try
    (let [md (s3/get-object-metadata p {:bucket-name bucket :key k})]
      (= etag (:etag md))
      #_true)
    (catch AmazonS3Exception e
      (if (= 404 (.getStatusCode e))
        nil
        (throw e)))))

(defn sync-missing-keys
  [cfg from-p from-bucket to-p to-bucket key-prefix]
  (let [from-ks-ch (all-bucket-keys from-p from-bucket key-prefix)
        mk-worker (fn [i]
                    (go-loop []
                      (when-let [[k etag] (<! from-ks-ch)]
                        (if-not (exists-etag-matches? to-p to-bucket k etag)
                          (do
                            (synchro-prn [i k etag])
                            (stream-object cfg from-p from-bucket k to-p to-bucket k))
                          (synchro-prn "Skipping" k))
                        (recur))))
        workers (mapv mk-worker (range 5))]
    (timbre/info "Starting sync...")
    (doseq [w workers] (<!! w))
    (timbre/info "Finished sync...")))

(defn -main
  [config & args]
  (timbre/info "Hello")
  (assert (.exists (io/file config)) (str "Config file " config " does not exist"))
  (let [cfg (load-file config)
        {:keys [from-profile to-profile from-bucket to-bucket key-prefix log]} cfg]
    (assert (valid-aws-creds? from-profile))
    (assert (valid-aws-creds? to-profile))
    (when log
      (timbre/merge-config! log))
    (sync-missing-keys cfg from-profile from-bucket to-profile to-bucket (or key-prefix ""))
    (shutdown-agents)))

