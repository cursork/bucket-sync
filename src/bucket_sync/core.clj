(ns bucket-sync.core
  "Very simple tool for syncing between two s3 buckets, either of which may be
  in an isolated region (e.g. China). Run as:

      lein run -- config/foo.clj

  See config/example.clj for valid options."
  (:require [clojure.core.async        :refer [<! <!! chan onto-chan go go-loop]]
            [clojure.data.codec.base64 :as b64]
            [clojure.java.io           :as io]
            [amazonica.aws.s3          :as s3]
            [taoensso.timbre           :as timbre])
  (:import java.security.MessageDigest
           com.amazonaws.services.s3.model.AmazonS3Exception))

(defn all-objects-in-bucket
  [p bucket-name]
  (loop [objs []]
    (let [{:keys [truncated? object-summaries]} (s3/list-objects p {:bucket-name bucket-name})
          new-objs (concat objs object-summaries)]
      (if truncated?
        (recur new-objs)
        new-objs))))

;; The MD5 sum is defined to be encoded as B64. So we just go straight to
;; MessageDigest and deal with bytes rather than use (for example) clj-digest
(defn md5-b64
  [bs]
  (let [md (MessageDigest/getInstance "MD5")]
    (.update md bs)
    (-> (.digest md)
        b64/encode
        (String.))))

(defn missing-keys
  [from-p from-bucket to-p to-bucket key-prefix]
  (let [keys-to-etags (fn [p b]
                        (->> (all-objects-in-bucket p b)
                             (filter #(.startsWith (:key %) key-prefix))
                             (reduce
                               #(assoc %1 (:key %2) (:etag %2))
                               {})))
        from (keys-to-etags from-p from-bucket)
        to   (keys-to-etags to-p   to-bucket)
        missing (fn [[k etag]]
                  (if (= (to k) (from k))
                    nil ;; Match
                    (if (contains? to k) ;; Contained but etag difference, overwrite
                      [k true]
                      [k false])))]
    (keep missing from)))

(defn stream-object
  [from-p from-bucket from-key to-p to-bucket to-key]
  (let [metadata (s3/get-object-metadata from-p {:bucket-name from-bucket :key from-key})
        stream   (:object-content (s3/get-object from-p {:bucket-name from-bucket :key from-key}))]
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

(defn sync-missing-keys
  [from-p from-bucket to-p to-bucket ks]
  (let [ks-ch     (chan)
        mk-worker (fn [i]
                    (go-loop []
                      (when-let [[k overwrite] (<! ks-ch)]
                        (if overwrite
                          (timbre/info (str "(" i ") Overwriting " k "..."))
                          (timbre/info (str "(" i ") Copying " k "...")))
                        (stream-object from-p from-bucket k to-p to-bucket k)
                        (recur))))
        workers (mapv mk-worker (range 5))]
    (timbre/info "Starting sync...")
    (onto-chan ks-ch ks)
    (doseq [w workers] (<!! w))
    (timbre/info "Finished sync...")))

(defn one-sync
  [from-p from-bucket to-p to-bucket key-prefix]
  (->> (missing-keys from-p from-bucket to-p to-bucket key-prefix)
       (sync-missing-keys from-p from-bucket to-p to-bucket)))

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
    (one-sync from-profile from-bucket to-profile to-bucket (or key-prefix ""))
    (shutdown-agents)))

