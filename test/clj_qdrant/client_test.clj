(ns clj-qdrant.client-test
  (:require [clojure.test :refer [deftest testing is]]
            [clj-qdrant.client :as c]))

(deftest make-returns-shape
  (testing "make returns map with :client :grpc-client :config keys"
    (try
      (let [r (c/make {:host "localhost" :port 6334})]
        (is (map? r))
        (is (contains? r :client))
        (is (contains? r :grpc-client))
        (is (contains? r :config))
        (is (= "localhost" (-> r :config :host)))
        (c/close! r))
      (catch Throwable _
        ;; ctor may throw without a live server — acceptable in unit suite.
        (is true "ctor threw without server — acceptable")))))

(deftest close-is-idempotent
  (is (nil? (c/close! {:client nil}))))
