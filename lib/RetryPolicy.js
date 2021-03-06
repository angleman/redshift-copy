var EventEmitter = require('events').EventEmitter;
var _l = require('lodash');
var scheduling = require('tempus-fugit').scheduling;

function random(start, end) {
    var range = end - start;
    return Math.floor((Math.random() * range) + start);
}

var MAX = Math.pow(2, 53);

//Exponential backoff: http://en.wikipedia.org/wiki/Exponential_backoff
module.exports.randomExponentialBackoff = function(retries) {

	// wait anywhere between zero to 2^retries inclusive (hence +1)
	return random(0, Math.pow(2, retries) + 1);
};

/*
	a simple 2^retries backoff calculation
*/
module.exports.exponentialBackoff = function(retries) {

	return Math.pow(2, retries);
};

/*
	time slots scale using log2(retries)
*/
module.exports.logarithmicProgression = function(retries) {

	return Math.floor( Math.log(retries) / Math.log(2) ) + 1;
};

module.exports.constantInterval = function(retries) {
	return 1;
};

/*

*/
module.exports.enableRetryPolicy = function(bulkInsert, options) {

	var retries = 0;

	var policyEmitter = new EventEmitter();

	options = options || {};

	if (typeof options.retryCalculation === 'string')
		options.retryCalculation = module.exports[options.retryCalculation];

	// TODO check why deepExtend doesn't merge functions...
	options.retryCalculation = options.retryCalculation || defaults.retryCalculation;

	_l.merge(options, defaults, _l.defaults);

	if (typeof options.retryCalculation !== 'function')
		throw new Error('invalid retryCalculation [' + options.retryCalculation + ']');

	bulkInsert.on('flush', onFlush);

	function onFlush(flushOp) {
		flushOp.once('error', onFlushOpError);
	}

	function onFlushOpError(error, flushOp) {

		if (retries > options.maxRetries) {
			policyEmitter.emit('no more retries', flushOp);
			return;
		}

		var timeSlots = options.retryCalculation(++retries);

		if (timeSlots > options.maxDelay)
			timeSlots = options.maxDelay;

		function retryFlush() {
			bulkInsert.retryFlush(flushOp);
		}

		var job = scheduling.schedule(timeSlots * options.timeSlot, retryFlush);

		policyEmitter.emit('next flush', flushOp, retries, job);
	}

	return policyEmitter;
};

var defaults = module.exports.defaults = {
	// default time slot is one second
	timeSlot: 1000,

	// a function that accepts the amount of retries and return how many time slots to wait
	retryCalculation: module.exports.logarithmicProgression,

	// maximum time slots delay for retrying.
	// this will override the results of the retry calculation
	maxDelay: 600,

	// limit the number of retries
	// -1 is infinite
	maxRetries: Infinity
};