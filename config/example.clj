;; environ is a dependency, so feel free to also do:
; (require '[environ.core :refer [env]])
; {:from-profile (:s3sync-from-profile env) ...}

{:from-profile {:profile "mycnaccount" :endpoint "cn-north-1"}
 :to-profile   {:profile "myeuaccount" :endpoint "eu-west-1"}
 :from-bucket  "test-sync-from"
 :to-bucket    "test-sync-to"
 :key-prefix   "myapp/uploads"}
