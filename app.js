var Bunnyhop = require('./lib/bunnyhop')
var fs = require('fs');

if (module == process.mainModule) {
	var configFile = process.env.BUNNYHOP_CONFIG || "/etc/bunnyhop.cfg";
	var config = JSON.parse(fs.readFileSync(configFile, 'utf8'));

	var bunhop = new Bunnyhop(config);
	bunhop.listen();
  console.log("bunnyhop started.");
}
