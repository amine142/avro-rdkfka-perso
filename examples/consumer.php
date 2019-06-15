#!/usr/bin/env php
<?php
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 *      

    High-level consumer

    This example shows how to use the high level consumer.

    Example #1 High-level consumer example
 */
require_once('../lib/avro.php');

$rk = new RdKafka\Consumer();
$rk->setLogLevel(LOG_DEBUG);
$rk->addBrokers("127.0.0.1");

$topic = $rk->newTopic("test_avro");

$topic->consumeStart(0, RD_KAFKA_OFFSET_BEGINNING);
$metadatas = $rk->getMetaData(false, $topic, -1);
echo $metadatas->getOrigBrokerId() .' '. $metadatas->getOrigBrokerName().PHP_EOL;

while (true) {
    $msg = $topic->consume(0, 1000);
    if ($msg) {
        if ($msg->err) {
            echo $msg->errstr(), "\n";
            break;
        } else {
            // Load the string data string
            $read_io = new AvroStringIO($msg->payload);
            $data_reader = new AvroDataIOReader($read_io, new AvroIODatumReader());
            echo "$msg->offset from binary string:\n";
            foreach ($data_reader->data() as $datum)
                echo var_export($datum, true) . "\n";
        }
    }
}
