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

require_once('../lib/avro.php');
$writers_schema_json = <<<_JSON
{"name":"member",
 "type":"record",
 "fields":[{"name":"member_id", "type":"int"},
           {"name":"member_name", "type":"string"}]}
_JSON;

$jose = array('member_id' => 1392, 'member_name' => 'Jose');
$maria = array('member_id' => 1642, 'member_name' => 'Maria');
$data = array($jose, $maria);
// Create a data string
// Create a string io object.
$io = new AvroStringIO();
// Create a datum writer object
$writers_schema = AvroSchema::parse($writers_schema_json);
var_dump($writers_schema); die();
$writer = new AvroIODatumWriter($writers_schema);
$data_writer = new AvroDataIOWriter($io, $writer, $writers_schema);
   
foreach ($data as $datum)
   $data_writer->append($datum);

$data_writer->close();

$binary_string = $io->string();
var_dump($binary_string);die();
$rk = new RdKafka\Producer();
$rk->setLogLevel(LOG_DEBUG);
$rk->addBrokers("127.0.0.1");

$topic = $rk->newTopic("test_avro");

$i=0;
while(true){
    sleep(1);
    $topic->produce(RD_KAFKA_PARTITION_UA, 0, $binary_string);
    $i++;
}
