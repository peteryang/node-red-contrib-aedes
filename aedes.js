/**
 * Copyright 2013,2014 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

module.exports = function (RED) {
  'use strict';
  const pg = require('pg');
  const aedes = require('aedes');
  const net = require('net');
  const tls = require('tls');
  const http = require('http');
  const https = require('https');
  const ws = require('websocket-stream');

  function AedesBrokerNode (config) {
    RED.nodes.createNode(this, config);
    this.mqtt_port = parseInt(process.env.AEDES_MQTT_PORT);
    var mqtt_config = {
      mqtt_port: parseInt(process.env.AEDES_MQTT_PORT)
    };

    var db_config = {
      host: process.env.AEDES_MQTT_DBHOST,
      user: process.env.AEDES_MQTT_DBUSER, 
      database: process.env.AEDES_MQTT_DATABASE, 
      password: process.env.AEDES_MQTT_DBPASSWORD, 
      max: 3, 
      idleTimeoutMillis: 30000
    };
    if(process.env.AEDES_PGSQL_USETLS !== undefined && process.env.AEDES_PGSQL_USETLS == "true"){
      config.ssl = {
        rejectUnauthorized: false
      }
    }

    this.pgpool = new pg.Pool(db_config);

    const node = this;

    const aedesSettings = {};

    const broker = new aedes.Server(aedesSettings);
    let server = net.createServer(broker.handle);

    server.once('error', function (err) {
      if (err.code === 'EADDRINUSE') {
        node.error('Error: Port ' + mqtt_config.mqtt_port + ' is already in use');
        node.status({ fill: 'red', shape: 'ring', text: 'node-red:common.status.disconnected' });
      } else {
        node.error('Error: Port ' + mqtt_config.mqtt_port + ' ' + err.toString());
        node.status({ fill: 'red', shape: 'ring', text: 'node-red:common.status.disconnected' });
      }
    });

    if (this.mqtt_port) {
      server.listen(this.mqtt_port, function () {
        node.log('Binding aedes mqtt server '+ mqtt_config.mqtt_port );
        node.status({ fill: 'green', shape: 'dot', text: 'node-red:common.status.connected' });
      });
    }

    const authenticate = function (client, username, password, callback) {
      // var authorized = (username === node.username && password.toString() === node.password);
      var authorized = true;
      if (authorized) { client.user = username; }
      callback(null, authorized);
    };

    broker.authenticate = authenticate;

    broker.on('client', function (client) {
      const msg = {
        topic: 'client',
        payload: {
          client: client
        }
      };
      node.send(msg);
    });

    broker.on('clientReady', function (client) {
      const msg = {
        topic: 'clientReady',
        payload: {
          client: client
        }
      };
      node.status({ fill: 'green', shape: 'dot', text: RED._('aedes-mqtt-broker.status.connected', { count: broker.connectedClients }) });
      node.send(msg);
    });

    broker.on('clientDisconnect', function (client) {
      const msg = {
        topic: 'clientDisconnect',
        payload: {
          client: client
        }
      };
      node.send(msg);
      node.status({ fill: 'green', shape: 'dot', text: RED._('aedes-mqtt-broker.status.connected', { count: broker.connectedClients }) });
    });

    broker.on('clientError', function (client, err) {
      const msg = {
        topic: 'clientError',
        payload: {
          client: client,
          err: err
        }
      };
      node.send(msg);
      node.status({ fill: 'green', shape: 'dot', text: RED._('aedes-mqtt-broker.status.connected', { count: broker.connectedClients }) });
    });

    broker.on('connectionError', function (client, err) {
      const msg = {
        topic: 'connectionError',
        payload: {
          client: client,
          err: err
        }
      };
      node.send(msg);
      node.status({ fill: 'green', shape: 'dot', text: RED._('aedes-mqtt-broker.status.connected', { count: broker.connectedClients }) });
    });

    broker.on('keepaliveTimeout', function (client) {
      const msg = {
        topic: 'keepaliveTimeout',
        payload: {
          client: client
        }
      };
      node.send(msg);
      node.status({ fill: 'green', shape: 'dot', text: RED._('aedes-mqtt-broker.status.connected', { count: broker.connectedClients }) });
    });

    broker.on('subscribe', function (subscription, client) {
      const msg = {
        topic: 'subscribe',
        payload: {
          topic: subscription.topic,
          qos: subscription.qos,
          client: client
        }
      };
      node.send(msg);
    });

    broker.on('unsubscribe', function (subscription, client) {
      const msg = {
        topic: 'unsubscribe',
        payload: {
          topic: subscription.topic,
          qos: subscription.qos,
          client: client
        }
      };
      node.send(msg);
    });

    /*
    broker.on('publish', function (packet, client) {
      var msg = {
        topic: 'publish',
        payload: {
          packet: packet,
          client: client
        }
      };
      node.send(msg);
    });
     */

    broker.on('closed', function () {
      node.debug('Closed event');
    });

    this.on('close', function (done) {
      broker.close(function () {
        node.log('Unbinding aedes mqtt server on port '+ mqtt_config.mqtt_port );
        server.close(function () {
          node.debug('after server.close(): ');
          done();
        });
      });
    });
  }

  RED.nodes.registerType('aedes broker', AedesBrokerNode, { });
};
