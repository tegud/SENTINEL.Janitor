var moment = require('moment');
var elasticsearch = require('elasticsearch');
var _ = require('lodash');
var http = require('http');
var fs = require('fs');

var host = 'pentlrges05';

var logLines = ['Run time: ' + moment().format('YYYY-MM-DD HH:mm:ss')];

function logToFile(message) {
	logLines.push(message);
}

function flushLogs() {
	logLines.push('Complete time: ' + moment().format('YYYY-MM-DD HH:mm:ss'));
	fs.writeFileSync(__dirname + '/lastRun.log', logLines.join('\r\n') + '\r\n', 'utf-8');
}

var rules = [
	{
		'days': { "from": 0, "to": 8 },
		'allocation': {
			'include': { 'tag': 'realtime' },
			'exclude': { 'tag': 'archive' },
			'require': { 'tag': '' },
			'total_shards_per_node': -1
		}
	},
	{
		'days': { "from": 8 },
		'allocation': {
			'include': { 'tag': 'archive' },
			'exclude': { 'tag': 'realtime' },
			'require': { 'tag': '' },
			'total_shards_per_node': -1
		}
	},
	{
		'days': { "from": 31 },
		'close': true
	}//,
	 // {
	 // 	'days': { "from": 160 },
	 // 	'delete': true
	 // }
];

var client = elasticsearch.Client({
	host: 'http://' + host + ':9200'
});

var logstashIndexRegex = /logstash\-([0-9]{4}\.[0-9]{2}\.[0-9]{2})/i

client.cat.indices().then(function(d) {
	var lines = d.split('\n');
	var today = moment().utc();

	logToFile('Found ' + lines.length + (lines.length === 1 ? ' index' : ' indicies'));

	var indicies = lines.map(function(line) {
		var logstashIndexMatch = logstashIndexRegex.exec(line);
		var isClosed = line.indexOf('close') > -1 && line.indexOf('open') < 0;
		
		if(logstashIndexMatch) {
			var indexDate = moment(logstashIndexMatch[1], 'YYYY-MM-DD');
			var ruleMatched;

			rules.forEach(function(rule) {
				var dayDiff = today.diff(indexDate, 'd');
				
				if(rule.days 
					&& (!rule.days.from || (rule.days.from && dayDiff >= rule.days.from))
					&& (!rule.days.to || (rule.days.to && dayDiff < rule.days.to))) {
					ruleMatched = rule;
				}
			});

			if(ruleMatched) {
				return {
					index: logstashIndexMatch[0],
					isClosed: isClosed,
					rule: ruleMatched
				};
			}
		}
	});

	var sortedTasks = _.sortBy(indicies, function(index) {
		if(!index) {
			return;
		}
		return index.index;
	});

	var indexTasks = sortedTasks.map(function(indexAndRule) {
		if(!indexAndRule) {
			return;
		}

		return function() {
			logToFile('index: ' + indexAndRule.index + ', applying rule: ' + JSON.stringify(indexAndRule.rule.allocation));
			
			return {
				then: function(callback) {
					if(!indexAndRule.rule.allocation && !indexAndRule.rule.close && !indexAndRule.rule.delete) {
						logToFile('Nothing to do, skipping');

						return callback();
					}

					var options = {
						port: 9200,
						hostname: host,
						method: 'PUT',
						path: '/' + indexAndRule.index + '/_settings'
					};

					if(indexAndRule.rule.close) {
						options.method = 'POST';
						options.path = '/' + indexAndRule.index + '/_close';

						if(indexAndRule.rule.close && indexAndRule.isClosed) {
							return callback();
						}
						else {
							logToFile('Closing...');
						}
					}
					
					if(indexAndRule.rule.delete) {
						options.method = 'DELETE';
						options.path = '/' + indexAndRule.index;

						logToFile('Deleting...');
					}

					var req = http.request(options,function(response) {
						response.on('data', function () { });

						response.on('end', function () {
							if((response.statusCode + '')[0] === '2') {
								callback();
							}
							else {
								callback(new Error('Error updating index settings'));
							}
						});
					});

					var indexSettings = { };

					if(!indexAndRule.rule.close) {
						_.each(indexAndRule.rule.allocation, function(shardAllocation, allocationRuleType) {
							if(typeof shardAllocation === 'object') {
								_.each(shardAllocation, function(rule, field) {
									indexSettings['index.routing.allocation.' + allocationRuleType + '.' + field] = rule;
								});							
							}
							else {
								indexSettings['index.routing.allocation.' + allocationRuleType] = shardAllocation;
							}
						});

						req.write(new Buffer(JSON.stringify(indexSettings)));
					}

					req.end();

					logToFile('--------------------');
					logToFile('Updating index: ' + indexAndRule.index);
					logToFile('With settings: ' + JSON.stringify(indexSettings, null, 4));

				}
			};
		};
	});

	function processTask() {
		var task = indexTasks.pop();

		if(!task) {
			return processTask();
		}

		task().then(function() {
			if(indexTasks.length) {
				logToFile('Update Completed, ' + indexTasks.length + ' ' + (indexTasks.length === 1 ? 'index' : 'indicies') + ', moving on to next...');
			}
			else {
				logToFile('All indicies updated, finished. (' + indexTasks.length + ')');
				flushLogs();
				process.exit(code=0);
			}

			processTask();
		});
	}

	processTask();
});
