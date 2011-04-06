var Bunnyhop = require('./lib/bunnyhop')

if (module == process.mainModule) {
	new Bunnyhop().listen();
  console.log("bunnyhop started.");
}
