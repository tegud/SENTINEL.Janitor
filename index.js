var moment = require('moment');
var elasticsearch = require('elasticsearch');
var _ = require('lodash');
var http = require('http');

var host = 'logs.laterooms.com';

var rules = [
	{
		'days': { "from": 0, "to": 10 },
		'allocation': {
			'include': { 'tag': '' },
			'exclude': { 'tag': '' },
			'require': { 'tag': '' },
			'total_shards_per_node': 3
		}
	},
	{
		'days': { "from": 10 },
		'allocation': {
			'include': { 'tag': '' },
			'exclude': { 'tag': 'realtime' },
			'require': { 'tag': '' },
			'total_shards_per_node': -1
		}
	}
];

var client = elasticsearch.Client({
	host: 'http://' + host + ':9200'
});

var logstashIndexRegex = /logstash\-([0-9]{4}\.[0-9]{2}\.[0-9]{2})/i

client.cat.indices().then(function(d) {
	var lines = d.split('\n');
	var today = moment().utc();

	console.log('Found ' + lines.length + (lines.length === 1 ? ' index' : ' indicies'));

	var indicies = lines.map(function(line) {
		var logstashIndexMatch = logstashIndexRegex.exec(line);
		
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
			console.log('index: ' + indexAndRule.index + ', applying rule: ' + JSON.stringify(indexAndRule.rule.allocation));
			
			return {
				then: function(callback) {
					var options = {
						port: 9200,
						hostname: host,
						method: 'PUT',
						path: '/' + indexAndRule.index + '/_settings'
					};

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
					req.end();

					console.log('--------------------');
					console.log('Updating index: ' + indexAndRule.index);
					console.log('With settings: ' + JSON.stringify(indexSettings, null, 4));

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
				console.log('Update Completed, ' + indexTasks.length + ' ' + (indexTasks.length === 1 ? 'index' : 'indicies') + ', moving on to next...');
			}
			else {
				console.log('All indicies updated, finished. (' + indexTasks.length + ')');
				return;
			}

			processTask();
		});
	}

	processTask();
});
