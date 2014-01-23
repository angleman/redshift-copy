module.exports = Monitor;

function Monitor(monitor, config) {
	this._monitor = monitor;
	this._config = config;
}

Monitor.prototype.flushLatencyListener = function(event) {

	var monitor = this._monitor;
	var config = this._config;

	return function(flushOp) {

		var metric = flushOp.uploadLatency + flushOp.queryLatency;
		var state;

		if (metric > config.flushLatencyThreshold * 2)
			state = 'critical';
		else if (metric > config.flushLatencyThreshold)
			state = 'warning';
		else
			state = 'ok';

		monitor.send({
			service:'flush latency',
			metric: metric,
			state: state,
			ttl: event.ttl,
			tags: ['performance', 'database', event.name]
		});
	};
};

Monitor.prototype.activeFlushOperationsListener = function(copyOp, event) {

	var monitor = this._monitor;
	var config = this._config;

	return function(flushOp) {

		var metric = copyOp.activeFlushOperations;
		var state;

		if (metric > config.activeFlushOpsThreshold * 2)
			state = 'critical';
		else if (metric > config.activeFlushOpsThreshold)
			state = 'warning';
		else
			state = 'ok';

		monitor.send({
			service: 'concurrent flush operations',
			metric: metric,
			state: state,
			ttl: event.ttl,
			tags: ['performance', 'database', event.name]
		});
	};
};

Monitor.prototype.copyQueryLatencyListener = function(event) {

	var monitor = this._monitor;
	var config = this._config;

	return function(flushOp) {

		var metric = flushOp.queryLatency;
		var state;

		if (metric > config.queryLatencyThreshold * 2)
			state = 'critical';
		else if (metric > config.queryLatencyThreshold)
			state = 'warning';
		else
			state = 'ok';

		monitor.send({
			service: 'copy query latency',
			metric: metric,
			state: state,
			ttl: event.ttl,
			tags: ['performance', 'database', event.name]
		});
	};
};

Monitor.prototype.uploadLatencyListener = function(event) {

	var monitor = this._monitor;
	var config = this._config;

	return function(flushOp) {

		var metric = flushOp.uploadLatency;
		var state;

		if (metric > config.uploadLatencyThreshold * 2)
			state = 'critical';
		else if (metric > config.uploadLatencyThreshold)
			state = 'warning';
		else
			state = 'ok';

		monitor.send({
			service: 's3 upload latency',
			metric: metric,
			state: state,
			ttl: event.ttl,
			tags: ['performance', 'database', event.name]
		});
	};
};

Monitor.prototype.retriesListener = function(event) {

	var monitor = this._monitor;
	var config = this._config;

	return function(flushOp, retries, job) {

		monitor.send({
			description: flushOp.key,
			service: 's3 upload latency',
			metric: retries,
			state: 'critical',
			ttl: event.ttl,
			tags: ['performance', 'database', event.name]
		});
	};
};

Monitor.prototype.exceptionListener = function(name, tags) {
	return this._monitor.exceptionListener(name, tags);
};