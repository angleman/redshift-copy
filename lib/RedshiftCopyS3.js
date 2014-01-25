module.exports = RedshiftCopyS3;

var assert = require('assert');
var async = require('async');
var uuid = require('node-uuid');
var path = require('path');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var ip = require('ip');
var domain = require('domain');
var FlushOperation = require('./FlushOperation.js');

var NULL = '\\N';
var DELIMITER = '|';
var NEWLINE = new Buffer('\n', 'utf8');
var EXTENSION = 'log';
var MAX = Math.pow(2, 53);
var ipAddress = ip.address();
var pid = process.pid;

/*
	@param datastore - a db-stuff datastore

	@param options - {
		fields: 			[an array of the table fields involved in the copy],
		delimiter: 			[delimiter to use when writing the copy files],
		tableName: 			[target table],
		extension: 			[the extension of the files written to s3],
		threshold: 			[the number of events that trigger a flush],
		idleFlushPeriod: 	[longtest time events will stay in the buffer before flush (if threshold is not met then this will be the time limit for flushing)],
		autoVacuum: 		[a boolean indicating if a vacuum operation should be executed after each insert],
		aws: 				{ clientType: ... region: ..., accessKeyId: ..., secretAccessKey: ..., bucket: ...}
	}

*/
util.inherits(RedshiftCopyS3, EventEmitter);
function RedshiftCopyS3(datastore, S3ClientProviderFactory, options) {
	EventEmitter.call(this);
	if (typeof(datastore) !== 'object')
		throw new Error('missing datastore');

	if (typeof(S3ClientProviderFactory) !== 'object')
		throw new Error('missing s3 client provider');

	if (typeof(options) !== 'object')
		throw new Error('missing options');

	if (options.delimiter === undefined)
		this.delimiter = DELIMITER;
	else
		this.delimiter = options.delimiter;

	if (typeof(options.tableName) !== 'string')
		throw new Error('missing or invalid table name');

	this._tableName = options.tableName;

	this._extension = options.extension || EXTENSION;

	if (!util.isArray(options.fields))
		throw new Error('missing fields');

	if (options.fields.length === 0)
		throw new Error('missing fields');

	this._fields = [].concat(options.fields);

	if (options.threshold === 0)
		throw new Error('cannot set threshold to 0');

	if (options.threshold === undefined)
		options.threshold = 1000;

	this._threshold = options.threshold;

	if (options.idleFlushPeriod === 0)
		throw new Error('cannot set idleFlushPeriod to 0');

	if (options.idleFlushPeriod === undefined)
		options.idleFlushPeriod = 5000;

	this._idleFlushPeriod = options.idleFlushPeriod;

	this._awsOptions = options.aws;

	if (this._awsOptions === undefined)
		throw new Error('missing aws options');

	if (this._awsOptions.accessKeyId === undefined)
		throw new Error('missing aws accessKeyId');

	if (this._awsOptions.bucket === undefined)
		throw new Error('missing aws bucket');

	if (this._awsOptions.secretAccessKey === undefined)
		throw new Error('missing aws secretAccessKey');

	var bucketParts = this._awsOptions.bucket.split('/');

	this._bucket = bucketParts.shift();

	this._keyPrefix = bucketParts.join('/');

	this._datastore = datastore;

	this._s3ClientProvider = S3ClientProviderFactory.newInstance(this._awsOptions.clientType, options);

	this._currentBufferLength = 0;

	this._buffer = [];

	this.activeFlushOperations = 0;

	this._ipAddress = ipAddress;

	this._pid = pid;
}

RedshiftCopyS3.prototype.insert = function(row) {
	if (row === undefined) return;
	if (row.length === 0) return;

	var text = '';
	for (var i = 0; i < row.length; i++) {
		if (i > 0)
			text += this.delimiter;

		text += this._escapeValue(row[i]);
	}

	var rowBuffer = new Buffer(text + NEWLINE, 'utf8');

	this._buffer.push(rowBuffer);
	this._currentBufferLength += rowBuffer.length;

	var flushOp;

	if (this._buffer.length === this._threshold) {

		this._stopIdleFlushMonitor();

		flushOp = this.flush();
	}

	this._startIdleFlushMonitor();

	return flushOp; // its ok that this is undefined when no flush occurs
};

RedshiftCopyS3.prototype.flush = function () {

	if (this._buffer.length === 0) return;

	var buffer = this._buffer;
	var bufferLength = this._currentBufferLength;
	this._buffer = [];
	this._currentBufferLength = 0;
	this.activeFlushOperations++;

	var filename = this._generateFilename();

	var key = this._generateKey(filename);

	var flushOp = this._newFlushOperation(this._bucket, key, buffer, bufferLength, this._generateCopyQuery(key));

	var self = this;

	flushOp.countDecreasedOnce = false;

	flushOp.on('error', onFlushEvent(this, flushOp));
	flushOp.on('success', onFlushEvent(this, flushOp));

	this.retryFlush(flushOp);

	return flushOp;
};

function onFlushEvent(bulkInsert, flushOp) {

	return function onFlushEventFunctor() {
		if (!flushOp.countDecreasedOnce) {
			flushOp.countDecreasedOnce = true;
			bulkInsert.activeFlushOperations--;
		}
	}
}

RedshiftCopyS3.prototype.retryFlush = function(flushOp) {

	this.emit('flush', flushOp);

	// using the provider instead of using the instance directly to prevent memory leaks
	flushOp.start(this._s3ClientProvider.get(this._bucket), this._datastore);
};

RedshiftCopyS3.prototype._newFlushOperation = function (bucket, key, buffer, bufferLength, copyQuery) {
	return new FlushOperation(bucket, key, buffer, bufferLength, copyQuery);
};

RedshiftCopyS3.prototype._startIdleFlushMonitor = function () {
	var self = this;

	// do not start if we're already started
	if (self._timeoutRef) return;

	self._timeoutRef = setTimeout(function timer() {
		self._timeoutRef = undefined;
		self.flush();

	}, self._idleFlushPeriod);

	self._timeoutRef.unref();
};

RedshiftCopyS3.prototype._stopIdleFlushMonitor = function () {
	clearTimeout(this._timeoutRef);
	this._timeoutRef = undefined;
};

RedshiftCopyS3.prototype._generateCopyQuery = function(key) {
	return 'COPY '
		+ this._tableName
		+ ' ('
		+ this._fields.join(', ')
		+ ')'
		+ ' FROM '
		+ "'"
		+ 's3://'
		+ this._bucket
		+ '/'
		+ key
		+ "'"
		+ ' CREDENTIALS '
		+ "'aws_access_key_id="
		+ this._awsOptions.accessKeyId
		+ ';'
		+ 'aws_secret_access_key='
		+ this._awsOptions.secretAccessKey
		+ "'"
		+ ' ESCAPE';
};

RedshiftCopyS3.prototype._generateKey = function(filename) {
	return this._keyPrefix + '/' + filename;
};

RedshiftCopyS3.prototype._createS3Client = function() {
	if (this._awsOptions === undefined)
		throw new Error('missing aws options');

	if (this._awsOptions.accessKeyId === undefined)
		throw new Error('missing aws accessKeyId')

	if (this._bucket === undefined)
		throw new Error('missing aws bucket');

	if (this._awsOptions.secretAccessKey === undefined)
		throw new Error('missing aws secretAccessKey');

	if (this._awsOptions.region === undefined)
		throw new Error('missing aws region');

	return knox.createClient({
		key: this._awsOptions.accessKeyId,
		secret: this._awsOptions.secretAccessKey,
		bucket: this._bucket,
		region: this._awsOptions.region
	});
};

RedshiftCopyS3.prototype._escapeValue = function(value) {
	if (value === null || value === undefined) {
		return NULL;
	}

	if (typeof(value) === 'string') {
		return value.replace(/\\/g, '\\\\').replace(/\|/g, '\\|');
	}

	return value;
};

RedshiftCopyS3.prototype._generateFilename = function () {
	return this._tableName + '-' + this._ipAddress + '-' + this._pid + '-' + this._now() + '-' + this._uuid() + '.' + this._extension;
};

RedshiftCopyS3.prototype._uuid = uuid;
RedshiftCopyS3.prototype._now = Date.now;