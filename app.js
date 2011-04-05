var Bunnyhop = require('./lib/bunnyhop')

if (module == process.mainModule) {
	var bunnyhop = new Bunnyhop();
	bunnyhop.listen();
  console.log("bunnyhop started.");
}
