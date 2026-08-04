(ns run
  "nbb entry point. The compiled cljs.main run is the other half — see
  test/columnar/cljs_runner.cljs. Both, because SCI interpretation cannot
  stand in for real compilation and vice versa."
  (:require [clojure.test :as t]
            [columnar.engine-test]))

(defmethod t/report [::t/default :end-run-tests] [m]
  (when-not (t/successful? m) (js/process.exit 1)))

(t/run-tests 'columnar.engine-test)
