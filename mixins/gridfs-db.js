"use strict";

var codec = require('../lib/object-codec.js');
var bodec = require('bodec');
var inflate = require('../lib/inflate');
var deflate = require('../lib/deflate');

var sha1 = require('git-sha1');
var modes = require('../lib/modes.js');
var client = require('mongodb').MongoClient;
var gridfs = require('mongodb').GridStore;
var ObjectID = require('mongodb').ObjectID;
var url = 'mongodb://localhost:27017/tedit';
var db;

mixin.init = init;

mixin.loadAs = loadAs;
mixin.saveAs = saveAs;
mixin.loadRaw = loadRaw;
mixin.saveRaw = saveRaw;
module.exports = mixin;

function mixin(repo, prefix) {
  if (!prefix) throw new Error("Prefix required");
  repo.refPrefix = prefix;
  repo.saveAs = saveAs;
  repo.saveRaw = saveRaw;
  repo.loadAs = loadAs;
  repo.loadRaw = loadRaw;
  repo.readRef = readRef;
  repo.updateRef = updateRef;
  repo.hasHash = hasHash;
}

function init(callback) {

  client.connect(url, function(err, conn) {
    if (err) { 
      callback(new Error("Problem initializing database"));
    } else {
      db = conn;
      callback();
    }
  });

}

function saveAs(type, body, callback) {
  /*jshint: validthis: true */
  if (!callback) return saveAs.bin(this, type, body);
  var has, buffer;
  try {
    buffer = codec.frame({type:type,body:body});
    hash = sha1(buffer);
  } catch (err) {
    return callback(err);
  }
  this.saveRaw(hash, buffer, callback);
}

function saveRaw(hash, buffer, callback) {
  /*jshint: validthis: true */
  if (!callback) return saveRaw.bind(this, hash, buffer);
  var gridStore = new GridStore(db, ObjectID(hash), 'w');
  gridStore.open(function(err, gridStore) {
    gridStore.write(bodec.toBase64(deflate(buffer)), function(err, gridStore) {
      gridStore.close(function(err, result) {
        if (err) {
          return callback(err);
        } else {
          callback(null, hash);
        }
      });
    });
  });
}
