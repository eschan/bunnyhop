var events = require('events')
var sys = require('sys')
var http = require('http')
var url = require('url')

module.exports = RestClient;

function RestClient() {
	events.EventEmitter.call(this);
}

RestClient.super_ = events.EventEmitter;
RestClient.prototype = Object.create(events.EventEmitter.prototype, {
	constructor: {
		value: RestClient,
		enumerable: false
	}
});

RestClient.prototype.post = function(uri, header, data) {
	var self = this;
	this.url = url.parse(uri);
	
	try {
		var site = http.createClient(this.url.port, this.url.hostname);
		site.on('error', function(err) {
		    self.emit('fail', err);
		});
		
		header.host = this.url.hostname;
		
		var request = site.request('POST', '/', header);
		
		var post_data = data;
		
		if(header['content-type'] == 'application/json')
			post_data = JSON.stringify(data);
		
		request.write(post_data); // this needs to be properly formatted json, or it will barf
		request.end();
		request.on('response', function(res) {
		    sys.debug('status code: ' + res.statusCode);
		    
		    res.on('data', function(data) {
		    	self.emit('success', res.statusCode, data);
		    });
		});
  } catch (e) {
  	sys.error(e);
  	request.end();
  }
	
	return self;
}

///////THIS IS TESTING CODE//////////
/*
var client = new RestClient();
var rest_client = client.post('localhost', 3001, {'host': 'localhost', 'content-type': 'application/json'}, {'hello': 'world'} );
rest_client.on('success', function(statusCode, data) {
	console.log(statusCode);
}).on('fail', function(err) {
	console.log('FAILED!!');
	console.log(err);
});
*/





