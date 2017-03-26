var readline = require('readline');
var http = require('http');
var fs = require('fs');

// color
// var color = require('colors-cli');
var color = require('colors-cli/safe')
var error = color.red.bold;
var warn = color.yellow;
var notice = color.blue;

//	PrettyTable
var figlet = require('figlet');
PrettyTable = require('prettytable');

//	bject.values
var es7_values = require('object.values');

var rl = readline.createInterface({
	input: process.stdin,
	output: process.stdout

});

// 关闭http服务
var closeHttpServer = function() {
	httpServer.close();
	console.log(error('\n关闭http服务!\n'));
}

// 渲染
var render = function(html) {
	httpServer = http.createServer(function(req, res) {
		res.writeHead(200, {
			'Content-Type': 'text/html;charset = utf8'
		});
		res.write('<style>table,tr,td,th{border: 1px solid #000;}</style>');
		res.write(html);
		res.end('');
	}).listen(3000);
	console.log(error("\nHTTP服务开启，监听3000端口......\n"));
}

// 清屏
var clearScreen = function() {
	var child = require("child_process");
	var clear_screen = child.exec("clear", function(err, stdout, stderr) {
		if (err) {
			console.log("clear_screen_error_stderr: " + stderr);
		} else {
			console.log("clear_screen_error_stdout: " + stdout);
		}

	});
	console.info(error('-------------------------------------------------'));
	console.log(notice('-------------------- 已清屏 ---------------------'));
	console.info(error('-------------------------------------------------'));

	// console.info(notice('-------------------------------------------------'));
	// console.log(notice('******************** 已清屏 *********************'));
	// console.info(notice('-------------------------------------------------'));
}



// // 检查用户
var checkUser = function(username, password) {
	var username = username;
	var password = password;
	var rs = fs.createReadStream('data/usersList.json');
	rs.setEncoding('utf8');
	rs.on('data', function(chunk) {
		var infos = JSON.parse(chunk);
		var usernameArr = [];
		var passwordArr = [];
		for (var i = 0; i < infos.length; i++) {
			usernameArr.push(infos[i]['username']);
			passwordArr.push(infos[i]['password']);
		}
		if (usernameArr.indexOf(username) !== -1 && passwordArr.indexOf(password) !== -1) {
			return true;
		} else {
			return false
		}

	});
}



var DB = {
	// this.user = username;
};

// 
DB.createTable = function() {
	var tablesList = [];
	var tableName = this.sqlStr.substring(13, this.sqlStr.indexOf("("));
	var kvMap = this.sqlStr.substring(this.sqlStr.indexOf("(") + 1, this.sqlStr.indexOf(")")).replace(/,/g, "").split(" ");


	// 创建表
	function createTable() {

		var rs = fs.createReadStream('data/tablesList.txt');
		rs.setEncoding('utf8');
		rs.on('data', function(chunk) {

			tablesList = chunk.split(',');

			if (tablesList.indexOf(tableName) !== -1) {
				console.log(notice(tableName) + error('已存在！无法创建该关系！'));
			} else {
				tablesList.push(tableName);
				var ws = fs.createWriteStream('data/tablesList.txt');
				ws.write(tablesList.toString());

				var arr = [];
				var obj = {};
				var table = {};
				for (var i = 0; i < kvMap.length; i++) {
					if (i % 2 == 0) {
						// obj[kvMap[i]] = '';
						obj[kvMap[i]] = kvMap[i + 1];
					}
				}
				arr.push(obj);
				table.name = tableName;
				//	默认拥有全部权限的用户 admin root
				table.users = ['ROOT', 'ADMIN'];
				table.data = arr;
				var ws = fs.createWriteStream(`data/${tableName}.json`);
				ws.write(JSON.stringify(table, null, 2));
				// console.info();
				console.log(error('创建关系 ' + tableName + ' 成功！'));
			}
		});
	}
	createTable();
};


// INSERT INTO S VALUES('S1','WANG',20,'M');
DB.insertTable = function() {
	var tableName = this.sqlStr.substring(12, this.sqlStr.indexOf("(") - 7);
	var vMap = this.sqlStr.substring(this.sqlStr.indexOf("(") + 1, this.sqlStr.indexOf(")")).replace(/'/g, "").split(',');

	function writeFileFromObj() {
		var rs = fs.createReadStream(`data/${tableName}.json`);
		rs.setEncoding('utf8');
		rs.on('data', function(chunk) {
			var name = '';
			var data = {};
			var table = {};
			var obj = {};
			table = JSON.parse(chunk);
			name = JSON.parse(chunk).name;
			data = JSON.parse(chunk).data;
			var obj0 = data[0];
			var i = 0;
			for (var k in obj0) {
				obj[k] = vMap[i];
				i++;
			}
			data.push(obj);
			table.name = name;
			table.data = data;
			var ws = fs.createWriteStream(`data/${tableName}.json`);
			ws.write(JSON.stringify(table, null, 2));
			// console.info();
			console.log(error('插入元组成功！'));
		});
	}
	writeFileFromObj();
};


// select * from t_admin
DB.selectTable = function() {
	var tableName = this.sqlStr.substring(14);

	function readFileToObj() {
		var rs = fs.createReadStream(`data/${tableName}.json`);
		rs.setEncoding('utf8');
		rs.on('data', function(chunk) {
			console.log(chunk);
			var name = '';
			var data = {};
			name = JSON.parse(chunk).name;
			data = JSON.parse(chunk).data;
			var html = `<h1>${name}</h1><table><tr>`;
			var kMap = data[0];
			for (var k in kMap) {
				html += `<th>${k}</th>`;
			}
			html += '</tr>';
			for (var i = 1; i < data.length; i++) {
				html += '<tr>';
				for (var k in data[i]) {
					html += `<td>${data[i][k]}</td>`
				}
				html += '</tr>';
			}
			html += '</table>';
			render(html);
		});
	}
	// readFileToObj();

	function renderView() {
		var rs = fs.createReadStream(`data/${tableName}.json`);
		rs.setEncoding('utf8');
		rs.on('data', function(chunk) {
			var colObj = JSON.parse(chunk).data[0];
			var dataArr = JSON.parse(chunk).data;
			var headers = Object.keys(colObj);
			var rows = [];
			for (var i = 1; i < dataArr.length; i++) {
				rows.push(es7_values(dataArr[i]));
			}
			// console.info();
			// console.info();
			// console.log('**********关系 '+tableName+' 视图**********');
			// console.info();
			//输出表格
			console.info();
			console.info();
			pt = new PrettyTable();
			pt.create(headers, rows);
			pt.print();

		});
	}

	renderView();
};


// drop table 删除表
DB.dropTable = function() {
	var tableName = this.sqlStr.substring(11);
	var rs = fs.createReadStream('data/tablesList.txt');
	rs.setEncoding('utf8');
	rs.on('data', function(chunk) {
		var tablesList = chunk.split(',');
		if (tablesList.indexOf(tableName) !== -1) {
			tablesList.splice(tablesList.indexOf(tableName), 1);
			var ws = fs.createWriteStream('data/tablesList.txt');
			ws.write(tablesList.join(','));
			var path = `./data/${tableName}.json`;
			fs.unlink(path, function() {
				// console.info();
				console.log(error('删除表 ' + tableName + ' 成功！'));
			});
		} else {
			console.log(notice(tableName) + error('不存在！无法删除该关系！'));
		}
	});
};


// alert table <关系名> add|drop <列名> <列类型>
DB.alterTable = function() {
	var arr = this.sqlStr.split(' ');
	var tableName = arr[2];
	var type = arr[3];
	var col = arr[4];

	function writeColToFile(type) {
		if (type === 'ADD') {
			var rs = fs.createReadStream(`data/${tableName}.json`);
			rs.setEncoding('utf8');
			rs.on('data', function(chunk) {
				var name = '';
				var data = {};
				var table = {};
				var obj = {};
				table = JSON.parse(chunk);
				name = JSON.parse(chunk).name;
				data = JSON.parse(chunk).data;
				var colArr = Object.keys(data[0]);
				if (colArr.indexOf(col) !== -1) {
					console.log(error('输入列 ' + col + ' 存在！不可添加！'));
				} else {
					for (var i = 0; i < data.length; i++) {
						data[i][col] = '';
					}
					table.name = name;
					table.data = data;
					var ws = fs.createWriteStream(`data/${tableName}.json`);
					ws.write(JSON.stringify(table, null, 2));
					console.log(error('插入列 ' + col + ' 成功！'));
				}
			});
		}

		if (type === 'DROP') {
			var rs = fs.createReadStream(`data/${tableName}.json`);
			rs.setEncoding('utf8');
			rs.on('data', function(chunk) {
				var name = '';
				var data = {};
				var table = {};
				var obj = {};
				table = JSON.parse(chunk);
				name = JSON.parse(chunk).name;
				data = JSON.parse(chunk).data;
				var colArr = Object.keys(data[0]);
				if (colArr.indexOf(col) === -1) {
					console.log(error('输入列 ' + col + ' 不存在！无法删除！'));
				} else {
					for (var i = 0; i < data.length; i++) {
						delete data[i][col];
					}
					table.name = name;
					table.data = data;
					var ws = fs.createWriteStream(`data/${tableName}.json`);
					ws.write(JSON.stringify(table, null, 2));
					console.log(error('删除列 ' + col + ' 成功！'));
				}
			});
		}
	}
	writeColToFile(type);
}


// delete from <关系名> where <条件表达式>
DB.deleteTable = function() {
	var arr = this.sqlStr.split(' ');
	var tableName = arr[2];
	var experArr = arr[4].split('=');
	var colObj = {};
	colObj[experArr[0]] = experArr[1];
	var colKey = experArr[0];
	var colVal = experArr[1];

	function delItemToFile() {
		var rs = fs.createReadStream(`data/${tableName}.json`);
		rs.setEncoding('utf8');
		rs.on('data', function(chunk) {
			var name = '';
			var data = [];
			var table = {};
			// var obj = {};
			table = JSON.parse(chunk);
			name = JSON.parse(chunk).name;
			data = JSON.parse(chunk).data;
			var colArr = Object.keys(data[0]);
			if (colArr.indexOf(colKey) === -1) {
				console.log(error('输入列 ' + notice(colKey) + error(' 不存在！无法删除！')));
			} else {

				var bFlag = false;
				// var resArr = [];
				var oriData = JSON.parse(chunk).data;
				var count = 0;
				for (var item of oriData) {
					for (var k in item) {
						if (k === colKey && item[colKey] === colVal) {
							// resArr.push(item);
							// console.log(data.indexOf(item));
							count++;
							data.splice(oriData.indexOf(item));
							bFlag = true;
						}
					}
				}
				if (!bFlag) {
					console.log(error('该条件 ' + notice(arr[4]) + error(' 无法查询出结果！无法进行删除操作！')));
				} else {
					console.log(error('删除 ' + count + ' 条数据!'));
				}

			}

			// console.log(data);
			table.name = name;
			table.data = data;
			var ws = fs.createWriteStream(`data/${tableName}.json`);
			ws.write(JSON.stringify(table, null, 2));

		});
	}

	delItemToFile();
}


// update <关系名> set <列名>=<常值> where <条件表达式>
DB.updateTable = function() {
	var arr = this.sqlStr.split(' ');
	var tableName = arr[1];
	var colArr = arr[3].split('=');
	var colKey = colArr[0];
	var colVal = colArr[1];
	var experArr = arr[5].split('=');
	var experKey = experArr[0];
	var experVal = experArr[1];

	function updateItemToFile() {
		var rs = fs.createReadStream(`data/${tableName}.json`);
		rs.setEncoding('utf8');
		rs.on('data', function(chunk) {
			var table = JSON.parse(chunk);
			var name = JSON.parse(chunk).name;
			var data = JSON.parse(chunk).data;

			if (!(experKey in data[0])) {
				console.log(error('输入条件 ' + notice(experKey) + error(' 不存在！无法更新！')));
			}
			if (!(colKey in data[0])) {
				console.log(error('输入列名 ' + notice(colKey) + error(' 不存在！无法更新！')));
			}
			if (experKey in data[0] && colKey in data[0]) {
				var count = 0;
				for (var item of data) {
					for (var k in item) {
						if (k === experKey && item[experKey] === experVal) {
							item[colKey] = colVal;
							count++;
						}
					}
				}
				table.name = name;
				table.data = data;
				var ws = fs.createWriteStream(`data/${tableName}.json`);
				ws.write(JSON.stringify(table, null, 2));
				console.log(error('更新 ' + count + ' 条数据!'));
			}
		});
	}

	updateItemToFile();
}


DB.createView = function() {
	var arr = this.sqlStr.split(' ');
	var viewName = arr[2];
	var tableName = arr[7]
	var sql = arr.splice(4).join(' ');

	function createView() {
		var rs = fs.createReadStream(`data/${tableName}.json`);
		rs.setEncoding('utf8');
		rs.on('data', function(chunk) {
			var colObj = JSON.parse(chunk).data[0];
			var dataArr = JSON.parse(chunk).data;
			var headers = Object.keys(colObj);
			var rows = [];
			for (var i = 1; i < dataArr.length; i++) {
				rows.push(es7_values(dataArr[i]));
			}
			console.info();
			console.info();
			console.log('**********关系 ' + tableName + ' 视图**********');
			console.info();
			//输出视图
			pt = new PrettyTable();
			pt.create(headers, rows);
			pt.print();
			var tableView = pt.toString();
			var ws = fs.createWriteStream(`view/${viewName}.view`);
			ws.write(tableView);
		});
	}
	createView();
}


DB.dropView = function() {
	var arr = this.sqlStr.split(' ');
	var viewName = arr[2];
	var path = `./view/${viewName}.view`;
	fs.unlink(path, function() {
		console.log(error('删除视图 ' + viewName + ' 成功！'));
	});
}


DB.showTables = function() {
	var rs = fs.createReadStream('data/tablesList.txt');
	rs.setEncoding('utf8');
	rs.on('data', function(chunk) {
		var tablesList = chunk.split(',');
		var headers = ['TABLES_IN_DBSQL'];
		var rows = [];
		for (var i = 0; i < tablesList.length; i++) {
			rows.push(new Array(tablesList[i]));
		}
		// console.info();
		// console.info();
		// console.log('**DBSQL 关系列表**');
		// console.info();
		//输出表格
		console.info();
		console.info();
		pt = new PrettyTable();
		pt.create(headers, rows);
		pt.print();
	});
}


//	CREATE USER 'username' IDENTIFIED BY 'password'; 
DB.createUser = function() {
	var arr = this.sqlStr.split(' ');
	var username = arr[2];
	var password = arr[5];

	function createUser() {
		var rs = fs.createReadStream('data/usersList.json');
		rs.setEncoding('utf8');
		rs.on('data', function(chunk) {
			var infos = JSON.parse(chunk);
			var arr = [];
			for (var i = 0; i < infos.length; i++) {
				arr.push(infos[i]['username']);
			}
			if (arr.indexOf(username) !== -1) {
				console.log(error('用户名 ' + username + ' 已存在，无法添加！'));
			} else {
				var info = {};
				info.username = username;
				info.password = password;
				info.powers = '';
				infos.push(info);
				var ws = fs.createWriteStream('data/usersList.json');
				ws.write(JSON.stringify(infos, null, 2));
				console.log(error('用户 ' + username + ' 创建成功！'));
			}

		});

	}

	createUser();
}


var grantPower = function(sql) {

	var arr = sql.toUpperCase().split(' ');
	var fn = arr[1];
	var tableName = arr[3];
	var username = arr[5];

	function cheackPower() {
		var powersList = ['CREATE', 'DROP', 'ALTER', 'DELETE', 'UPDATE', 'SELECT', 'SHOW', 'GRANT', 'REVOKE'];
		if (powersList.indexOf(fn) === -1) {
			console.info();
			console.log(error('输入的权限关键字 ') + notice(fn) + error(' 不存在! 权限关键字有：CREATE,DROP,ALTER,DELETE,UPDATE,SELECT,SHOW,GRANT,REVOKE'));
			return false;
		} else {
			return true;
		}
	}

	function checkTableAndUser() {
		var isPower = cheackPower();
		if (isPower) {
			var rs = fs.createReadStream('data/tablesList.txt');
			rs.setEncoding('utf8');
			rs.on('data', function(chunk) {
				var tablesList = chunk.split(',');
				if (tablesList.indexOf(tableName) === -1) {
					console.info();
					console.log(error('输入的关系 ') + notice(tableName) + error(' 不存在! 无法完成grant操作！'));
				} else {
					var rs = fs.createReadStream('data/usersList.json');
					rs.setEncoding('utf8');
					rs.on('data', function(chunk) {
						var infos = JSON.parse(chunk);
						var arr = [];
						for (var i = 0; i < infos.length; i++) {
							arr.push(infos[i]['username']);
						}
						if (arr.indexOf(username) === -1) {
							console.info();
							console.log(error('用户 ' + notice(username) + error(' 不存在！无法完成grant操作！')));

						} else {
							grantUserToTable();
							grantPowerToUser();
							console.info();
							console.log(error('权限赋予操作 grant 成功！'));
						}
					});
				}
			});
		}
	}



	function grantUserToTable() {
		var rs = fs.createReadStream(`data/${tableName}.json`);
		rs.setEncoding('utf8');
		rs.on('data', function(chunk) {
			table = JSON.parse(chunk);
			if (table.users.indexOf(username) === -1) {
				table.users.push(username);
				var ws = fs.createWriteStream(`data/${tableName}.json`);
				ws.write(JSON.stringify(table, null, 2));
				// console.info();
				console.log(error('用户 ') + notice(username) + error(' 被赋予关系 ') + notice(tableName) + error(' 操作权限！'));

			} else {
				//console.log(error('用户 ')+notice(username)+error(' 已经被赋予关系 ')+notice(tableName)+error(' 操作权限！请勿重复添加！'));
			}
		});
	}

	function grantPowerToUser() {
		var rs = fs.createReadStream('data/usersList.json');
		rs.setEncoding('utf8');
		rs.on('data', function(chunk) {
			users = JSON.parse(chunk);
			var userArr = [];
			for (var i = 0; i < users.length; i++) {
				userArr.push(users[i]['username']);
			}
			var obj = users[userArr.indexOf(username)];
			if (obj.powers.indexOf(fn) !== -1) {
				console.log(error(fn + ' 已存在!无法添加！'));
			} else {

				if (obj.powers === '') {
					obj.powers = [];
					obj.powers.push(fn);
				} else {
					obj.powers.push(fn);
				}
				users[userArr.indexOf(username)] = obj;
				// console.log(users);
				var ws = fs.createWriteStream('data/usersList.json');
				ws.write(JSON.stringify(users, null, 2));
				console.log(error('添加 ' + fn + ' 操作成功！'));
			}

		});

	}

	checkTableAndUser();

};


DB.revoke = function() {
	var arr = this.sqlStr.split(' ');
	var fn = arr[1];
	var tableName = arr[3];
	var username = arr[5];

	function revokeUserToTable() {
		var rs = fs.createReadStream(`data/${tableName}.json`);
		rs.setEncoding('utf8');
		rs.on('data', function(chunk) {
			table = JSON.parse(chunk);
			if (table.users.indexOf(username) !== -1) {
				table.users.splice(table.users.indexOf(username));
				// console.log(table);
				var ws = fs.createWriteStream(`data/${tableName}.json`);
				ws.write(JSON.stringify(table, null, 2));
				console.info();
				console.log(error('用户 ') + notice(username) + error(' 撤销成功'));
			} else {
				console.info();
				// console.log(error('用户不存在！') + notice(username) + error(' 无法进行撤销操作！'));
			}
		});

	}

	revokeUserToTable();

	function revokePowerToUser() {
		var rs = fs.createReadStream('data/usersList.json');
		rs.setEncoding('utf8');
		rs.on('data', function(chunk) {
			users = JSON.parse(chunk);
			var userArr = [];
			for (var i = 0; i < users.length; i++) {
				userArr.push(users[i]['username']);
			}
			var obj = users[userArr.indexOf(username)];
			if (obj.powers.indexOf(fn) !== -1) {
				obj.powers.splice(obj.powers.indexOf(fn));
				var ws = fs.createWriteStream('data/usersList.json');
				ws.write(JSON.stringify(users, null, 2));
				console.info();
				console.log(error('撤销 ' + fn + ' 操作成功！'));
			} else {
				console.info();
				console.log(notice(fn) + error(' 操作不存在！撤销失败！'));
			}

		});

	}
	revokePowerToUser();

}



DB.init = function(sql, powers) {
	// 检查用户权限

	if (powers === 'ALL') {

		this.sqlStr = sql.toString().toUpperCase();
		var createTable = this.sqlStr.startsWith('CREATE TABLE');
		var dropTable = this.sqlStr.startsWith('DROP TABLE');
		var alterTable = this.sqlStr.startsWith('ALTER TABLE');
		var insertTable = this.sqlStr.startsWith('INSERT INTO');
		var deleteTable = this.sqlStr.startsWith('DELETE FROM');
		var updateTable = this.sqlStr.startsWith('UPDATE');
		var selectTable = this.sqlStr.startsWith('SELECT');
		var createView = this.sqlStr.startsWith('CREATE VIEW');
		var dropView = this.sqlStr.startsWith('DROP VIEW');
		var createIndex = this.sqlStr.startsWith('CREATE INDEX');
		var dropIndex = this.sqlStr.startsWith('DROP INDEX');
		var createUser = this.sqlStr.startsWith('CREATE USER');
		var grant = this.sqlStr.startsWith('GRANT');
		var revoke = this.sqlStr.startsWith('REVOKE');
		var showTables = this.sqlStr.startsWith('SHOW TABLES');
		if (createTable) {
			this.createTable();
		} else if (dropTable) {
			this.dropTable();
		} else if (alterTable) {
			this.alterTable();
		} else if (insertTable) {
			this.insertTable();
		} else if (deleteTable) {
			this.deleteTable();
		} else if (updateTable) {
			this.updateTable();
		} else if (selectTable) {
			this.selectTable();
		} else if (createView) {
			this.createView();
		} else if (dropView) {
			this.dropView();
		} else if (createIndex) {
			this.createIndex();
		} else if (dropIndex) {
			this.dropIndex();
		} else if (createUser) {
			this.createUser();
		} else if (grant) {
			grantPower(this.sqlStr);
		} else if (revoke) {
			this.revoke();
		} else if (showTables) {
			this.showTables();
		} else {
			console.info();
			console.log(error('输入有误!'));
			console.info();
		}

	} else {
		var isCreate = powers.includes('CREATE');
		var isDrop = powers.includes('DROP');
		var isAlter = powers.includes('ALTER');
		var isDelete = powers.includes('DELETE');
		var isUpdate = powers.includes('UPDATE');
		var isSelect = powers.includes('SELECT');
		var isShow = powers.includes('SHOW');
		var isGrant = powers.includes('GRANT');
		var isRevoke = powers.includes('REVOKE');
		this.sqlStr = sql.toString().toUpperCase();


		
		if (isCreate) {
			var createTable = this.sqlStr.startsWith('CREATE TABLE');
			var createView = this.sqlStr.startsWith('CREATE VIEW');
			var createIndex = this.sqlStr.startsWith('CREATE INDEX');
			var createUser = this.sqlStr.startsWith('CREATE USER');
			if (createTable) {
				this.createTable();
			} else if (createView) {
				this.createView();
			} else if (createIndex) {
				this.createIndex();
			} else if (createUser) {
				this.createUser();
			} else {
				// console.log('无 create 权限');
			}
		} else {
			// console.log('无 create 权限');
		}


		if (isDrop) {
			var dropTable = this.sqlStr.startsWith('DROP TABLE');
			var dropView = this.sqlStr.startsWith('DROP VIEW');
			var dropIndex = this.sqlStr.startsWith('DROP INDEX');
			if (dropTable) {
				this.dropTable();
			} else if (dropView) {
				this.dropView();
			} else if (dropIndex) {
				this.dropIndex();
			} else {
				// console.log('无 drop 权限');
			}
		} else {
			// console.log('无 drop 权限');
		}



		if (isAlter) {
			var alterTable = this.sqlStr.startsWith('ALTER TABLE');
			if (alterTable) {
				this.alterTable();
			} else {
				// console.log('无 alter 权限');
			}
		} else {
			// console.log('无 alter 权限');
		}



		if (isDelete) {
			var deleteTable = this.sqlStr.startsWith('DELETE FROM');
			if (deleteTable) {
				this.deleteTable();
			} else {
				// console.log('无 delete 权限');
			}
		} else {
			// console.log('无 delete 权限');
		}

		if (isUpdate) {
			var updateTable = this.sqlStr.startsWith('UPDATE');
			if (updateTable) {
				this.updateTable();
			} else {
				// console.log('无 update 权限');
			}
		} else {
			// console.log('无 update 权限');
		}

		if (isSelect) {
			var selectTable = this.sqlStr.startsWith('SELECT');
			if (selectTable) {
				this.selectTable();
			} else {
				// console.log('无 select 权限');
			}
		} else {
			// console.log('无该 select 权限');
		}

		if (isShow) {
			var showTables = this.sqlStr.startsWith('SHOW TABLES');
			if (showTables) {
				this.showTables();
			} else {
				// console.log('无 show 权限');
			}
		} else {
			// console.log('无 show 权限');
		}

		if (isGrant) {
			var grant = this.sqlStr.startsWith('GRANT');
			if (grant) {
				grantPower(this.sqlStr);
			} else {
				// console.log('无 grant 权限');
			}
			
		} else {
			// console.log('无 grant 权限');
		}

		if (isRevoke) {
			var revoke = this.sqlStr.startsWith('REVOKE');
			if (revoke) {
				this.revoke();
			} else {
				// console.log('无 revoke 权限');
			}
			
		} else {
			// console.log('无 revoke 权限');
		}
	}

}



function power(isPower, powers) {
	rl.setPrompt("DB_SQL > ");
	rl.prompt();
	rl.on('line', function(input) {
		var sql = input.trim();
		switch (sql) {
			case 'end':
				closeHttpServer();
				break;
			case 'exit':
				// console.log(error("\nDB_SQL程序退出!\n"));
				console.log(error("\n****************************************************************\n" +
						"*                         DB_SQL 程序退出!                     *" +
						"\n****************************************************************\n"));

				rl.close();
				process.exit(0);
				break;
			case '':
				break;
			case 'clear':
				clearScreen();
				break;
			case 'help':
				console.log("\n****************************************\n" + "\n·help      ----      帮助信息\n·info      ----      有关信息\n·end       ----      关闭http服务\n·sql       ----      sql语句示例\n·clear     ----      清屏\n·exit      ----      退出\n" + "\n****************************************\n");
				break;
			case 'sql':
				console.log("\n********************************************************************************************\n" + "\n·创建关系 CREATE TABLE              ----      CREATE TABLE DEMO(NO INT, NAME CHAR, AGE INT)\n·删除关系 DROP TABLE                ----      DROP TABEL DEMO\n·添加|删除列属性 ALTER TABLE        ----      ALTER TABLE DEMO ADD|DROP SEX CHAR\n·插入 INSERT INTO                   ----      INSERT INTO DEMO VALUES(1001,DBNAME,12) \n·删除 DELETE                        ----      DELETE FROM DEMO WHERE NAME=TOM\n·更新 UPDATE                        ----      UPDATE DEMO SET NAME=DBMS WHERE NO=1\n·创建视图 CREATE VIEW               ----      CREATE VIEW DEMO AS SELECT * FROM TABLE\n·删除视图 DROP VIEW                 ----      DROP VIEW DEMO\n·列出所有关系 SHOW TABLES           ----      SHOW TABLES\n·创建用户 CREATE USER               ----      CREATE USER USERNAME IDENTIFIED BY PASSWORD\n·赋予权限 GRANT                     ----      GRANT SELECT ON DEMO TO USERNAME\n·撤销权限 REVOKE                    ----      REVOKE SELECT ON DEMO FROM USERNAME\n" + "\n********************************************************************************************\n");
				break;
			case 'info':
				figlet.text('DB SQL', {
					// font: 'Cyberlarge',
					// font: 'Impossible',
					font: 'ANSI Shadow',
					horizontalLayout: 'default',
					verticalLayout: 'default'
				}, function(err, data) {
					if (err) {
						console.dir(err);
						return;
					}
					console.info();
					console.log('\nv.1.0.0   Author:Liuwei(20123349)   Data:2017/03/25\n');
					console.log(data);
					console.log(notice("\n****************************************************************\n" +
						"*      数据库系统原理实验演示！输入'help'查看更多帮助信息！    *" +
						"\n****************************************************************\n"));
				});
				break;
			default:
				if (isPower) {
					DB.init(sql, powers);
				} else if (!isPower && (sql.startsWith('grant') || sql.startsWith('GRANT'))) {
					grantPower(sql);
				} else {
					console.info();
					console.log(error('当前用户无操作权限!'));
					console.info();
				}
		}
		rl.prompt();
	});
}

console.log(notice("\n****************************************************************\n" +
	"*         数据库系统DB_SQL登录页面！请输入用户名和密码！       *" +
	"\n****************************************************************\n"));
console.log("请输入用户名和密码（输入格式为：用户名@密码，例如：admin@123456） 要想使用默认用户登录请输入 :root\n");


rl.question(">>>>>", function(input) {
	if (input === ':root') {
		console.log(notice("\n****************************************************************\n" +
			"*  登录成功！欢迎进入DB_SQL系统！输入'info'查看基本信息信息！  *" +
			"\n****************************************************************\n"));
		power(1, 'ALL');
	} else {
		if (input.split('@').length === 2) {
			var username = input.split('@')[0].toUpperCase();
			var password = input.split('@')[1].toUpperCase();
			var rs = fs.createReadStream('data/usersList.json');
			rs.setEncoding('utf8');
			rs.on('data', function(chunk) {
				var infos = JSON.parse(chunk);
				var usernameArr = [];
				var passwordArr = [];
				for (var i = 0; i < infos.length; i++) {
					usernameArr.push(infos[i]['username']);
					passwordArr.push(infos[i]['password']);
				}
				if (usernameArr.indexOf(username) !== -1 && passwordArr.indexOf(password) !== -1) {
					console.log(notice("\n****************************************************************\n" +
						"*  登录成功！欢迎进入DB_SQL系统！输入'info'查看基本信息信息！  *" +
						"\n****************************************************************\n"));
					console.log('当前用户：' + username);
					var powers = infos[usernameArr.indexOf(username)].powers;
					var isPowers;
					if (!powers) {
						console.log('拥有权限：NULL (没有任何权限，必须授予权限)');
						console.info();
						isPowers = 0;
						power(isPowers, powers);
					} else {
						console.log('拥有权限：' + powers);
						console.info();
						isPowers = 1;
						power(isPowers, powers);
					}

				} else {
					console.log(error('\n登录失败！') + warn('用户名或密码错误！') + error('DB_SQL程序退出!\n'));
					rl.close();
					process.exit(0);
				}

			});

		} else {
			console.log(error('\n输入格式错误! DB_SQL程序退出!\n'));
			rl.close();
			process.exit(0);
		}

	}

});