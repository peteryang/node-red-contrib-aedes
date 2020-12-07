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
  const http = require('http');
  const ws = require('websocket-stream');

  function AedesBrokerNode (config) {
    RED.nodes.createNode(this, config);
    this.mqtt_port = parseInt(process.env.AEDES_MQTT_PORT);
    this.mqtt_ws_port = parseInt(process.env.AEDES_MQTT_WS_PORT);
    var mqtt_config = {
      mqtt_port: parseInt(process.env.AEDES_MQTT_PORT),
      mqtt_ws_port: parseInt(process.env.AEDES_MQTT_WS_PORT),
      mqtt_accesskey_query: process.env.AEDES_MQTT_ACCESSKEY_QUERY
    };

    var db_config = {
      host: process.env.AEDES_MQTT_DBHOST,
      user: process.env.AEDES_MQTT_DBUSER, 
      database: process.env.AEDES_MQTT_DATABASE, 
      password: process.env.AEDES_MQTT_DBPASSWORD, 
      max: 3, 
      connectionTimeoutMillis: 5000,
      idleTimeoutMillis: 30000
    };
    if(process.env.AEDES_PGSQL_USETLS && process.env.AEDES_PGSQL_USETLS === "true"){
      db_config.ssl = {
        rejectUnauthorized: false
      }
    }

    this.pgpool = new pg.Pool(db_config);
    const node = this;


    const aedesSettings = {};

    const broker = new aedes.Server(aedesSettings);
    let server = net.createServer(broker.handle);

    let wss = null;
    let httpServer = null;

    if (this.mqtt_ws_port) {
      // Awkward check since http or ws do not fire an error event in case the port is in use
      const testServer = net.createServer();
      testServer.once('error', function (err) {
        if (err.code === 'EADDRINUSE') {
          node.error('Error: Port ' + mqtt_config.mqtt_ws_port + ' is already in use');
        } else {
          node.error('Error creating net server on port ' + mqtt_config.mqtt_ws_port + ', ' + err.toString());
        }
      });
      testServer.once('listening', function () {
        testServer.close();
      });

      testServer.once('close', function () {
        httpServer = http.createServer();
        
        wss = ws.createServer({
            server: httpServer
          }, broker.handle);
        httpServer.listen(mqtt_config.mqtt_ws_port, function () {
            node.log('Binding aedes mqtt server on ws port: ' + mqtt_config.mqtt_ws_port);
          });
        });
      testServer.listen(mqtt_config.mqtt_ws_port, function () {
        node.log('Checking ws port: ' + mqtt_config.mqtt_ws_port);
      });
    }

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
      if(client && client.req && client.req.upgrade){
        callback(null, true);
        return;
      }      
      if(!username || username.trim().length === 0){
        callback(null, false);
        return;
      }
      if(!password || password.toString().trim().length === 0){
        callback(null, false);
        return;
      }      
 
      var query = mqtt_config.mqtt_accesskey_query.replace("{1}", username);
      node.pgpool.query(query, (err, res) => {
        if(res && res.rows && res.rows.length === 1){
          var authorized = (password.toString().trim() === res.rows[0].accesskey.trim());
          if (authorized) { client.user = username; }
          callback(null, authorized);
        }else{
          callback(null, false);
        }
      });
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
      console.warn(err);
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

    broker.on('publish', function (packet, client) {
      var msg = {
        topic: 'publish',
        payload:  {
          packet: packet,
          // client: client
        }
      };
      if(client!=null && packet!= null){
        msg.payload.clientid=client.id;
        msg.payload.username=client.user;
          if(packet.topic.includes("/api/gateway/"+client.user)){
            node.send(msg);
          }
        }
    });

    broker.on('closed', function () {
      node.debug('Closed event');
    });
    
    this.on('close', function (done) {
      broker.close(function () {
        node.log('Unbinding aedes mqtt server from port: ' + mqtt_config.mqtt_port);
        node.pgpool.end();
        server.close(function () {
          node.debug('after server.close(): ');
          if (wss) {
            node.log('Unbinding aedes mqtt server from ws port: ' + mqtt_config.mqtt_ws_port);
            wss.close(function () {
              node.debug('after wss.close(): ');
              httpServer.close(function () {
                node.debug('after httpServer.close(): ');
                done();
              });
            });
          } else {
            done();
          }
        });
      });
    });

  }

  RED.nodes.registerType('aedes-iot-broker', AedesBrokerNode, { });
};
