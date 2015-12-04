;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns backtype.storm.command.update-topology
  (:use [clojure.tools.cli :only [cli]])
  (:use [backtype.storm thrift config log])
  (:import [backtype.storm.generated RebalanceOptions UpdateOptions]
           [backtype.storm StormSubmitter]
           [backtype.storm.utils Utils]
           [org.json.simple JSONValue])
  (:gen-class))

(defn -main [& args] 
  (let [[{jar :jar update-config :update-config} [name] _]
                  (cli args ["-j" "--jar" :default nil]
                            ["-u" "--update-config" :default nil])
        opts (UpdateOptions.)]

    ;;if jar param is provided, upload the jar and set the update options.
    (when jar (.set_uploadedJarLocation opts (StormSubmitter/submitJar (Utils/readStormConfig) jar)))
    ;;the file provided in --update-config will not be used to actually make the nimbus connection.
    ;; we will read both the command line options and the file provided via -c option and merge them and the option
    ;; specified in the file will take precedence.
    (let [cmd-line-config (into {} (Utils/readCommandLineOpts))
          config (JSONValue/toJSONString (merge cmd-line-config
                                    (when update-config (Utils/findAndReadConfigFile update-config true))))]
      (when (not-empty config))
      (log-message "these configs will be updated " config)
        (.set_jsonConf opts config))

    (with-configured-nimbus-connection nimbus
      (.updateTopology nimbus name opts)
      (log-message "Topology " name " is updating. Topology updates happen gradually so you should expect a some delay before
      all workers are updated."))))
