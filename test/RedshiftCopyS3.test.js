var RedshiftCopyS3 = require('../lib/RedshiftCopyS3.js');
var assert = require('assert');
var fs = require('fs');
var path = require('path');
var $u = require('util');
var _l = require('lodash');
var PolyMock = require('polymock');
var dbStuff = require('db-stuff');
var EventEmitter = require('events').EventEmitter;
var testutil = require('./lib/testutil.js')

var testroot = path.join(__dirname, 'testfiles');
var logfile = path.join(testroot, 'testlog');

var testRow = [1,2,3,'root', null];
var options = {
	path: testroot,
	tableName: 'liga',
	threshold: 3,
	idleFlushPeriod: 100000,
	fields: ['a', 'b']
};

var awsOptions = {
	region: 'us-standard',
	accessKeyId: '2',
	secretAccessKey: '3',
	bucket: 'asd'
};


function createMock(threshold) {
	var mock = PolyMock.create();

	mock.dummy = new EventEmitter();
	mock.dummy.start = function () {};
	mock.createMethod('_escapeValue', undefined, { dynamicValue: function(val) { return val.toString(); }});
	mock.createMethod('_startIdleFlushMonitor');
	mock.createMethod('_stopIdleFlushMonitor');
	mock.createMethod('flush', mock.dummy);
	mock.createMethod('_generateFilename', testutil.EXPECTED_FILENAME);
	mock.createMethod('_generateCopyQuery', testutil.EXPECTED_COPY_QUERY);
	mock.createMethod('_generateKey', testutil.EXPECTED_KEY);
	mock.createMethod('_newFlushOperation', mock.dummy);
	mock.createMethod('_uuid', '123');
	mock.createMethod('_now', 'now');
	mock.createMethod('emit');
	mock.createMethod('retryFlush');

	mock.createProperty('_fields', [ 'a', 'b' ]);
	mock.createProperty('_awsOptions', { accessKeyId: '1', secretAccessKey: '2' });
	mock.createProperty('_bucket', testutil.EXPECTED_BUCKET);
	mock.createProperty('_buffer', []);
	mock.createProperty('_currentBufferLength', 0);
	mock.createProperty('activeFlushOperations', 0);
	mock.createProperty('delimiter', '|');
	mock.createProperty('_threshold', threshold);
	mock.createProperty('_keyPrefix', 'prefix');
	mock.createProperty('_tableName', 'tablename');
	mock.createProperty('_extension', 'log');
	mock.createProperty('_pid', '1');
	mock.createProperty('_ipAddress', '1.1.1.1');
	mock.createProperty('_datastore', {});
	mock.createProperty('_s3ClientProvider', { get: function() {
		return {};
	}});

	return mock;
}

function createS3ClientProviderFactoryMock() {
	var mock = PolyMock.create();

	mock.createMethod('newInstance', {});

	return mock;
}

describe('RedshiftCopyS3', function() {

	describe('constructor', function () {

		it('throws an error if no datastore is specified', function () {
			try {
				var rbl = new RedshiftCopyS3();
				throw new Error('constructor should have thrown an error');
			} catch (e) {
				assert.strictEqual(e.message, 'missing datastore');
			}
		});

		it('throws an error if no s3 client provider is specified', function () {
			try {
				var rbl = new RedshiftCopyS3(testutil.createDatastoreMock());
				throw new Error('constructor should have thrown an error');
			} catch (e) {
				assert.strictEqual(e.message, 'missing s3 client provider');
			}
		});

		it('throws an error if no options are specified', function () {
			try {
				var rbl = new RedshiftCopyS3(testutil.createDatastoreMock(), createS3ClientProviderFactoryMock());
				throw new Error('constructor should have thrown an error');
			} catch (e) {
				assert.strictEqual(e.message, 'missing options');
			}
		});

		it('throws an error if no table name is not supplied in the options', function () {
			try {
				var rbl = new RedshiftCopyS3(testutil.createDatastoreMock(), createS3ClientProviderFactoryMock(),  {});
				throw new Error('constructor should have thrown an error');
			} catch (e) {
				assert.strictEqual(e.message, 'missing or invalid table name');
			}
		});

		it('throws an error if fields are missing in options', function () {
			try {
				var rbl = new RedshiftCopyS3(testutil.createDatastoreMock(), createS3ClientProviderFactoryMock(), { tableName: '123' });
				throw new Error('constructor should have thrown an error');
			} catch (e) {
				assert.strictEqual(e.message, 'missing fields');
			}
		});

		it('throws an error if fields are missing in options', function () {
			try {
				var rbl = new RedshiftCopyS3(testutil.createDatastoreMock(), createS3ClientProviderFactoryMock(), { tableName: '123', fields: [] });
				throw new Error('constructor should have thrown an error');
			} catch (e) {
				assert.strictEqual(e.message, 'missing fields');
			}
		});

		it('throws an error if threshold is set to 0', function () {
			try {
				var options = { idleFlushPeriod: 0, tableName: 'asdlj', fields: [ '1' ], threshold: 0 };
				var rbl = new RedshiftCopyS3(testutil.createDatastoreMock(), createS3ClientProviderFactoryMock(), options);
				throw new Error('constructor should have thrown an error');
			} catch (e) {
				assert.strictEqual(e.message, 'cannot set threshold to 0');
			}
		});

		it('throws an error if idleFlushPeriod is set to 0', function () {
			try {
				var options = { idleFlushPeriod: 0, tableName: 'asdlj', fields: [ '1' ], threshold: 10 };
				var rbl = new RedshiftCopyS3(testutil.createDatastoreMock(), createS3ClientProviderFactoryMock(), options);
				throw new Error('constructor should have thrown an error');
			} catch (e) {
				assert.strictEqual(e.message, 'cannot set idleFlushPeriod to 0');
			}
		});

		//TODO complete constructor / initial state tests
	});

	it('generates filenames used in the copy process', function () {
		var mock = createMock(1);

		var filename = RedshiftCopyS3.prototype._generateFilename.call(mock.object);
		assert.strictEqual(filename, 'tablename-1.1.1.1-1-now-123.log');
	});

	it('generates a copy query', function () {
		var mock = createMock(1);

		var query = RedshiftCopyS3.prototype._generateCopyQuery.call(mock.object, '1.log');

		assert.strictEqual(query, 'COPY tablename (a, b) FROM \'s3://mybucket/1.log\' CREDENTIALS \'aws_access_key_id=1;aws_secret_access_key=2\' ESCAPE');
	});

	describe('insert', function () {

		it('saves a row in a buffer', function() {
			var buff = new Buffer('1|2|3\n', 'utf8');
			var mock = createMock(10);
			var row = [1, 2, 3];

			RedshiftCopyS3.prototype.insert.call(mock.object, row);

			assert.strictEqual(mock.invocations[0].method, '_escapeValue');
			assert.strictEqual(mock.invocations[0].arguments[0], 1);

			assert.strictEqual(mock.invocations[1].property, 'delimiter');
			assert.strictEqual(mock.invocations[1].value, '|');
			assert.strictEqual(mock.invocations[1].operation, 'get');

			assert.strictEqual(mock.invocations[2].method, '_escapeValue');
			assert.strictEqual(mock.invocations[2].arguments[0], 2);

			assert.strictEqual(mock.invocations[3].property, 'delimiter');
			assert.strictEqual(mock.invocations[3].value, '|');
			assert.strictEqual(mock.invocations[3].operation, 'get');

			assert.strictEqual(mock.invocations[4].method, '_escapeValue');
			assert.strictEqual(mock.invocations[4].arguments[0], 3);

			assert.strictEqual(mock.invocations[5].property, '_buffer');
			assert.deepEqual(mock.invocations[5].value[0], buff);
			assert.strictEqual(mock.invocations[5].operation, 'get');

			assert.strictEqual(mock.invocations[6].property, '_currentBufferLength');
			assert.strictEqual(mock.invocations[6].value, 0);
			assert.strictEqual(mock.invocations[6].operation, 'get');

			assert.strictEqual(mock.invocations[7].property, '_currentBufferLength');
			assert.strictEqual(mock.invocations[7].value, 6);
			assert.strictEqual(mock.invocations[7].operation, 'set');

			assert.strictEqual(mock.invocations[8].property, '_buffer');
			assert.deepEqual(mock.invocations[8].value[0], buff);
			assert.strictEqual(mock.invocations[8].operation, 'get');

			assert.strictEqual(mock.invocations[9].property, '_threshold');
			assert.strictEqual(mock.invocations[9].value, 10);
			assert.strictEqual(mock.invocations[9].operation, 'get');

			assert.strictEqual(mock.invocations[10].method, '_startIdleFlushMonitor');

			RedshiftCopyS3.prototype.insert.call(mock.object, row);

			assert.strictEqual(mock.object._buffer.length, 2);
		});

		it('flushes the buffer when the threshold is reached, returning a flush operation object', function () {
			var buff = new Buffer('1|2|3\n', 'utf8');
			var mock = createMock(1);
			var row = [1, 2, 3];

			var flushOp = RedshiftCopyS3.prototype.insert.call(mock.object, row);

			assert.strictEqual(flushOp, mock.dummy);

			var len = mock.invocations.length;

			assert.strictEqual(mock.invocations[len - 3].method, '_stopIdleFlushMonitor');
			assert.strictEqual(mock.invocations[len - 2].method, 'flush');
			assert.strictEqual(mock.invocations[len - 1].method, '_startIdleFlushMonitor');
		});
	});

	describe('manage the idle flush monitor', function () {

		it('starts the monitor', function (done) {
			var flushCalled = false;

			var mock = {
				_idleFlushPeriod: 1000,
				flush: function () {
					flushCalled = true;
				}
			};

			RedshiftCopyS3.prototype._startIdleFlushMonitor.call(mock);

			assert.ok(typeof(mock._timeoutRef) === 'object');

			setTimeout(function () {
				assert.strictEqual(flushCalled, false, 'flush should not have been called yet');

				setTimeout(function () {
					assert.strictEqual(flushCalled, true, 'flush should not have been called by now');
					done();
				}, 400);
			}, 800);
		});

		it('does not restart it if it was already started', function () {

			var mock = {
				_idleFlushPeriod: 1000,
				flush: function () {}
			};

			RedshiftCopyS3.prototype._startIdleFlushMonitor.call(mock);

			assert.strictEqual(typeof(mock._timeoutRef), 'object');

			var expectedRef = mock._timeoutRef;

			RedshiftCopyS3.prototype._startIdleFlushMonitor.call(mock);

			assert.strictEqual(mock._timeoutRef, expectedRef);
		});

		it('stops the monitor', function () {
			var mock = {
				_timeoutRef: {}
			};

			RedshiftCopyS3.prototype._stopIdleFlushMonitor.call(mock);

			assert.strictEqual(mock._timeoutRef, undefined);
		});

		//TODO really bad test, improve
		it('stops the monitor when a flush operation occurs', function () {
			var stopCalled = false;
			var mock = createMock(1);

			RedshiftCopyS3.prototype.insert.call(mock.object, ['123']);

			var called = false;
			for (var i = 0; i < mock.invocations.length; i++) {
				if (mock.invocations[i].method === '_stopIdleFlushMonitor') {
					called = true;
					break;
				}
			}

			assert.ok(called);
		});
	});

	describe('flushing', function () {

		it('only happens if buffer has elements in it', function () {

			var mock = createMock(1);

			var flushOp = RedshiftCopyS3.prototype.flush.call(mock.object);

			assert.strictEqual(flushOp, undefined);
		});

		it('resets the state of the buffer', function () {
			var mock = createMock(2);

			mock.createProperty('_buffer', [ '1|2|3\n', '1|2|3\n' ]);
			mock.createProperty('_currentBufferLength', 9999);

			var flushOp = RedshiftCopyS3.prototype.flush.call(mock.object);

			assert.strictEqual(mock.object._buffer.length, 0);
			assert.strictEqual(mock.object._currentBufferLength, 0);
			assert.strictEqual(mock.object.activeFlushOperations, 1);
		});

		it('"copies" the current state of the buffer to a flushop object using the factory method _newFlushOperation', function () {

			var mock = createMock(2);
			var expectedBuffer = [ '1|2|3\n', '1|2|3\n' ];
			var expectedBufferLength = 9999;

			mock.createProperty('_buffer', expectedBuffer );
			mock.createProperty('_currentBufferLength', expectedBufferLength);

			var flushOp = RedshiftCopyS3.prototype.flush.call(mock.object);
			assert.strictEqual(flushOp, mock.dummy);

			var actual = mock.invocations[mock.invocations.length - 2].arguments;

			assert.strictEqual(actual[0], testutil.EXPECTED_BUCKET);
			assert.strictEqual(actual[1], testutil.EXPECTED_KEY);
			assert.deepEqual(actual[2], expectedBuffer);
			assert.strictEqual(actual[3], expectedBufferLength);
			assert.strictEqual(actual[4], testutil.EXPECTED_COPY_QUERY);

		});

		it('decreases the flush operations count when a flush operation is done', function () {

			var mock = createMock(2);

			var expectedBuffer = [ '1|2|3\n', '1|2|3\n' ];
			var expectedBufferLength = 9999;

			mock.createProperty('_buffer', expectedBuffer);
			mock.createProperty('_currentBufferLength', expectedBufferLength);

			var flushOp = RedshiftCopyS3.prototype.flush.call(mock.object);
			assert.strictEqual(flushOp, mock.dummy);

			flushOp.emit('success');

			var actual = mock.invocations.pop();

			assert.strictEqual(actual.property, 'activeFlushOperations');

			assert.strictEqual(actual.operation, 'set');

			assert.strictEqual(actual.property, 'activeFlushOperations');

			assert.strictEqual(actual.value, 0);
		});

		it('decreases the flush operations count when a flush operation has errors', function () {

			var mock = createMock(2);

			var expectedBuffer = [ '1|2|3\n', '1|2|3\n' ];
			var expectedBufferLength = 9999;

			mock.createProperty('_buffer', expectedBuffer);
			mock.createProperty('_currentBufferLength', expectedBufferLength);

			var flushOp = RedshiftCopyS3.prototype.flush.call(mock.object);
			assert.strictEqual(flushOp, mock.dummy);

			flushOp.emit('error');

			var actual = mock.invocations.pop();

			assert.strictEqual(actual.property, 'activeFlushOperations');

			assert.strictEqual(actual.operation, 'set');

			assert.strictEqual(actual.property, 'activeFlushOperations');

			assert.strictEqual(actual.value, 0);
		});

		// it.skip('will fail if code tries to decrease the flush op count more than once', function () {

		// 	var mock = createMock(2);

		// 	var expectedBuffer = [ '1|2|3\n', '1|2|3\n' ];
		// 	var expectedBufferLength = 9999;

		// 	mock.createProperty('_buffer', expectedBuffer);
		// 	mock.createProperty('_currentBufferLength', expectedBufferLength);

		// 	var flushOp = RedshiftCopyS3.prototype.flush.call(mock.object);
		// 	assert.strictEqual(flushOp, mock.dummy);

		// 	flushOp.emit('error');

		// 	var actual = mock.invocations.pop();

		// 	assert.strictEqual(actual.property, 'activeFlushOperations');

		// 	assert.strictEqual(actual.operation, 'set');

		// 	assert.strictEqual(actual.property, 'activeFlushOperations');

		// 	assert.strictEqual(actual.value, 0);

		// 	try {
		// 		flushOp.emit('error');
		// 		throw new Error('this should have failed');
		// 	} catch(e) {
		// 		assert.ok(e);
		// 	}
		// });
	});
});