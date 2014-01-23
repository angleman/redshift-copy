module.exports = FlushOperation;

var async = require('async');
var util = require('util');
var EventEmitter = require('events').EventEmitter;

util.inherits(FlushOperation, EventEmitter);
function FlushOperation(bucket, key, buffer, bufferLength, copyQuery) {
	EventEmitter.call(this);

	this.copyQuery = copyQuery;
	this.bucket = bucket;
	this.key = key;
	this.buffer = buffer;
	this.bufferLength = bufferLength;
}

FlushOperation.prototype.start = function (s3Client, datastore) {

	//TODO add checks for AbstractS3Client and DatastoreBase
	if (arguments.length < 2)
		throw new Error('missing arguments');

	this.flushStart = Date.now();

	this.uploadLatency = 0;
	this.queryLatency = 0;

	async.waterfall([

		this._uploadToS3(s3Client),
		this._updateUploadLatency(),
		this._executeCopyQuery(datastore),
		this._updateQueryLatency()

	], this.done());
};

/*
	override to change how flush op prepares the buffer before upload
*/
FlushOperation.prototype._prepareBuffer = function (buffer, bufferLength) {
	return Buffer.concat(buffer, bufferLength);
};

FlushOperation.prototype._updateUploadLatency = function() {
	var self = this;
	return function(uploadData, callback) {
		self.uploadLatency = Date.now() - self.uploadStart;
		callback(null, uploadData);
	};
};

FlushOperation.prototype._updateQueryLatency = function() {
	var self = this;
	return function(queryResults, callback) {
		self.queryLatency = Date.now() - self.queryStart;
		callback(null, queryResults);
	};
};

FlushOperation.prototype._uploadToS3 = function (s3Client) {
	var self = this;

	return function(callback) {

		self.uploadStart = Date.now();
		self.stage = '_uploadToS3';

		var buffer = self._prepareBuffer(self.buffer, self.bufferLength);

		function putCallback(err, res) {
			if (err) {
				callback(err);
				return;
			}

			if (res.statusCode !== 200) {
				callback('Response status code should be equals to 200 but was ' + res.statusCode);
				return;
			}

			callback(null, res);
		}

		s3Client.put(self.key, buffer, putCallback);
	};
};

FlushOperation.prototype._executeCopyQuery = function (datastore) {
	var self = this;

	return function(data, callback) {
		self.queryStart = Date.now();
		self.stage = '_executeCopyQuery';
		datastore.query(self.copyQuery, callback);
	};
};

FlushOperation.prototype.done = function () {

	var self = this;
	return function (err, results) {

		self.queryResults = results;

		if (err) {
			self.emit('error', err, self);
		} else {
			self.emit('success', self);
		}
	};
};