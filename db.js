/**
 * 
 * @File     数据库管理系统实验
 * @Author   Liuwei
 * @OS       macOS Sierra v10.12.4
 * @SDE      node v6.10.0
 * @Date     2017-04-27
 * @Version  v5.0.0
 * 
 */

// node_modules
var readline = require('readline');
var http = require('http');
var fs = require('fs');

// REPL color
var color = require('colors-cli/safe');
// var color = require('colors-cli');
var error = color.red.bold;
var warn = color.yellow;
var notice = color.blue;

//	REPL DB PrettyTable
var figlet = require('figlet');
PrettyTable = require('prettytable');

//	ES7 object.values
var es7_values = require('object.values');

//  ES2015 Object.assign() ponyfill
const objectAssign = require('object-assign');

//  REPL
var rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout

});

/**
 * [closeHttpServer 关闭http服务]
 */

var closeHttpServer = function() {
    httpServer.close();
    console.log(error('\n关闭http服务!\n'));
}

/**
 * [render 渲染HTML页面]
 * @param  {[String]} html 
 */

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

/**
 * [clearScreen REPL清屏]
 */

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



/**
 * 检查用户名
 * @param  {用户名 - [username]}
 * @param  {密码 - [password]}
 * @return {[boolean]}
 */

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

};


/**
 * createTable
 * @fn    创建新关系模式
 * @sql   CREATE TABLE <关系名>(<列名><列类型>,...,<列名><列类型>) 
 * @demo  CREATE TABLE STUDENTS(NO INT, NAME CHAR, AGE INT)
 */

DB.createTable = function() {
    var self = this;
    var tablesList = [];
    var tableName = self.sqlStr.substring(13, self.sqlStr.indexOf("("));
    var dictStr = self.sqlStr.substring(self.sqlStr.indexOf("(") + 1, self.sqlStr.lastIndexOf(")"));
    var kvMap = self.sqlStr.substring(self.sqlStr.indexOf("(") + 1, self.sqlStr.lastIndexOf(")")).replace(/,/g, ",").split(',');
    var dictString = (tableName + " " + dictStr).toLowerCase();

    // 创建表
    function createTable() {
        var rs = fs.createReadStream('data/tablesList.txt');
        rs.setEncoding('utf8');
        rs.on('data', function(chunk) {
            tablesList = chunk.split(',');
            if (tablesList.indexOf(tableName) !== -1) {
                console.log(notice(tableName) + error('已存在！无法创建该关系！'));
            } else {
                // create table demo(no int not null,name char,age int,primary key ('no'))
                // 获取主键 PRIMARY KEY
                if (self.sqlStr.indexOf('PRIMARY KEY') !== -1) {
                    var primaryKey = kvMap[kvMap.length - 1].substring(kvMap[kvMap.length - 1].indexOf("(") + 1, kvMap[kvMap.length - 1].indexOf(")")).replace(/'/g, "");
                    kvMap.splice(kvMap.length - 1, 1);
                }
                var obj = {};
                var table = {};
                var dict = {};
                var arr = [];
                var obj1 = {};
                tablesList.push(tableName);
                var ws = fs.createWriteStream('data/tablesList.txt');
                ws.write(tablesList.toString());
                for (var i = 0; i < kvMap.length; i++) {
                    var varr = kvMap[i].split(" ");
                    for (var j = 0; j < varr.length; j++) {
                        var dictObj = {};
                        dictObj['type'] = varr[1];
                        if (varr[j].indexOf('NOT') && varr[j].indexOf('NULL')) {
                            dictObj['default NULL'] = 'NOT';
                        } else {
                            dictObj['default NULL'] = 'YES';
                        }

                        obj[varr[0]] = dictObj;
                    }
                    //****
                    for (var k = 0; k < varr.length; k++) {
                        obj1[varr[0]] = varr[1];
                    }
                }
                arr.push(obj1);
                table.name = tableName;
                table.dict = obj;
                table.dict['primary key'] = primaryKey;
                //console.log(JSON.stringify(table, null, 2));
                // 默认拥有全部权限的用户 admin root
                table.users = ['ROOT', 'ADMIN'];
                table.index = [];
                table.data = arr;
                var ws = fs.createWriteStream(`data/${tableName}.json`);
                ws.write(JSON.stringify(table, null, 2));
                // console.info();
                console.log(error('创建关系 ' + tableName + ' 成功！'));
                var rs = fs.createReadStream(`dict/Database.dict`);
                rs.setEncoding('utf8');
                rs.on('data', function(chunk) {
                    var headers = ['Field', 'Type', 'NULL', 'Remark'];
                    var rows = [];
                    var dictObj = obj;
                    for (var key in dictObj) {
                        var a = [];
                        if (key === dictObj['primary key']) {
                            a[3] = "primary key";
                        } else {
                            a[3] = " - ";
                        }
                        a[0] = key;
                        a[1] = dictObj[key]['type'];
                        a[2] = dictObj[key]['default NULL'];
                        rows.push(a);
                    }
                    rows.pop();
                    pt = new PrettyTable();
                    pt.create(headers, rows);
                    var dictToFile = '关系 ' + tableName + ' 的数据字典: ' + dictString + '\n' + pt.toString();
                    //
                    var ws1 = fs.createWriteStream(`dict/${tableName}.dict`);
                    ws1.write(dictToFile);
                    //
                    chunk = chunk + '\n\n' + dictToFile;
                    var ws = fs.createWriteStream(`dict/Database.dict`);
                    ws.write(chunk);
                })
            }
        });
    }
    createTable();

};



/**
 * insertTable
 * @fn    插入元组
 * @sql   INSERT INTO <关系名>[(<列名>,...,<列名>)] (VALUES (<常值>,...,<常值>)) | SELECT 语句		  
 * @demo  INSERT INTO STUDENTS VALUES('220011','LIHUA',20)
 */

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



/**
 * selectTable
 * @fn    查询操作
 * @sql   SELECT [DISTICT] <属性表> FROM R1[<别名>],R2[<别名>],...,R3[<别名>] WHERE φ
 * @demo  SELECT * FROM STUDENTS
 */

DB.selectTable = function() {
    var self = this;
    var sql = self.sqlStr;

    var reg1 = /select\s+\*{1}\s+from\s+(\w+)\s*$/i;
    var reg2 = /select\s+((\w|#*\s*\,*\s*\w*)+)\s+from\s+(\w+)\s*$/i;
    var reg3 = /select\s+\*{1}\s+from\s+(\w+)\s+where\s+((\w+|#*\s*=\s*\w+\s*(and)*\s*)+)$/i;
    var reg4 = /selects\s*((\w*\#*\,*)+)\s*from\s*(\w*)\s*where\s*((\w+|#*\s*=\s*\w+\s*(and)*\s*)+)$/i;
    var reg5 = /select\s*\*{1}\s*from\s*((\w+\,*)+)$/i;
    var reg6 = /select\s+(((\w+\.+\w+\#*)\s*(\,)*\s*)+)\s+from\s+((\w+\,*)+)$/i;
    var reg7 = /select\s*\*{1}\s*from\s*((\w+\s*\,*\s*)+)\s*where\s*((\w+|.*|#*\s*=\s*\w+|.*|#*\s*)+)$/i;
    var reg8 = /select\s+(((\w+\.+\w+\#*)\s*(\,)*\s*)+)\s+from\s*((\w+\s*\,*\s*)+)\s*where\s*((\w+|.*|#*\s*=\s*\w+|.*|#*\s*)+)$/i;
    var reg9 = /selects\s*\*{1}\s*from\s*((\w+\s*\,*\s*)+)\s*where\s*((\w+|.*|#*\s*=\s*\w+\s*(and)*\s*)+)$/i;

    
    var reg3_index = /select@index\s+\*{1}\s+from\s+(\w+)\s+where\s+((\w+|#*\s*=\s*\w+\s*(and)*\s*)+)$/i;


    if (reg1.test(sql)) {
        // 全关系选择
        var rs = sql.match(reg1);
        var tableName = rs[1];
        var rs = fs.createReadStream('data/tablesList.txt');
        rs.setEncoding('utf8');
        rs.on('data', function(chunk) {
            tablesList = chunk.split(',');
            if (tablesList.indexOf(tableName) !== -1) {
                console.info();
                console.time('>>>>查询耗时: ');
                selectAll(tableName);
                console.timeEnd('>>>>查询耗时: ');
            } else {
                console.info();
                console.log('关系 ' + tableName + ' 不存在，无法完成查询操作！');
            }
        });
    } else if (reg2.test(sql)) {
        // 单关系的投影
        var rs = sql.match(reg2);
        var keyArr = rs[1].replace(/\s/gi, '').split(',');
        var tableName = rs[3];
        var rs = fs.createReadStream('data/tablesList.txt');
        rs.setEncoding('utf8');
        rs.on('data', function(chunk) {
            tablesList = chunk.split(',');
            if (tablesList.indexOf(tableName) !== -1) {
                console.info();
                console.time('>>>>查询耗时: ');
                selectFields(tableName, keyArr);
                console.timeEnd('>>>>查询耗时: ');
            } else {
                console.info();
                console.log('关系 ' + tableName + ' 不存在，无法完成查询操作！');
            }
        });
    } else if (reg3.test(sql)) {
        // 单关系的选择
        var rs = sql.match(reg3);
        var tableName = rs[1];
        var experArr = rs[2].replace(/\s/gi, '').split('AND');
        var rs = fs.createReadStream('data/tablesList.txt');
        rs.setEncoding('utf8');
        rs.on('data', function(chunk) {
            tablesList = chunk.split(',');
            if (tablesList.indexOf(tableName) !== -1) {

                console.info();
                console.time('>>>>查询耗时: ');
                selectAllByMulExp(tableName, experArr);
                console.timeEnd('>>>>查询耗时: ');

            } else {
                console.info();
                console.log('关系 ' + tableName + ' 不存在，无法完成查询操作！');
            }
        });
    } else if (reg4.test(sql)) {
        // 单关系的选择和投影
        var rs = sql.match(reg4);
        var colNameArr = rs[1].split(',');
        var tableName = rs[3];
        var experArr = rs[4].replace(/\s/gi, '').split('AND');
        var rs = fs.createReadStream('data/tablesList.txt');
        rs.setEncoding('utf8');
        rs.on('data', function(chunk) {
            tablesList = chunk.split(',');
            if (tablesList.indexOf(tableName) !== -1) {


                console.info();
                console.time('>>>>查询耗时: ');
                selectFieldsByMulExp(tableName, colNameArr, experArr);
                console.timeEnd('>>>>查询耗时: ');

            } else {
                console.info();
                console.log('关系 ' + tableName + ' 不存在，无法完成查询操作！');
            }
        });
    } else if (reg5.test(sql)) {
        // 多关系笛卡尔积
        var rs = sql.match(reg5);
        var tableNameArr = rs[1].split(',');
        var rs = fs.createReadStream('data/tablesList.txt');
        rs.setEncoding('utf8');
        rs.on('data', function(chunk) {
            tablesList = chunk.split(',');
            var isAllIn = tableNameArr.every(function(item, index, array) {
                return tablesList.indexOf(item) !== -1;
            });
            if (isAllIn) {
                console.info();
                console.time('>>>>查询耗时: ');
                selectAllByMulTable(tableNameArr);
                console.timeEnd('>>>>查询耗时: ');
            } else {
                console.log('关系不存在，无法完成查询操作！');
            }
        });
    } else if (reg6.test(sql)) {
        var rs = sql.match(reg6);
        var kvMap = rs[1].replace(/\s/gi, '').split(',');
        var tableNameArr = rs[5].split(',');
        var rs = fs.createReadStream('data/tablesList.txt');
        rs.setEncoding('utf8');
        rs.on('data', function(chunk) {
            tablesList = chunk.split(',');
            var isAllIn = tableNameArr.every(function(item, index, array) {
                return tablesList.indexOf(item) !== -1;
            });

            function readKey(tableName) {
                var filePath = `data/${tableName}.json`;
                var encoding = "utf-8";
                if (fs.existsSync(filePath)) {
                    var chunk = fs.readFileSync(filePath, encoding);
                    var data = JSON.parse(chunk).data[0];
                    return data;
                }
            }

            function isContained(a, b) {
                if (!(a instanceof Array) || !(b instanceof Array)) return false;
                if (a.length < b.length) return false;
                var aStr = a.toString();
                for (var i = 0, len = b.length; i < len; i++) {
                    if (aStr.indexOf(b[i]) == -1) return false;
                }
                return true;
            }

            if (isAllIn) {
                console.time('>>>>查询耗时: ');
                var kvArr = [];
                var kvTable = [];
                var rv = [];
                kvMap.forEach(function(item, index, array) {
                    kvArr.push(item.split('.'));
                    kvTable.push(item.split('.')[0]);
                    rv.push(item.split('.')[1]);
                });

                var colList = [];
                tableNameArr.forEach(function(item, index, array) {
                    var a = [];
                    a.unshift(item);
                    a.push(...Object.keys(readKey(item)));
                    colList.push(a);

                });

                var isAllSearchIn = kvTable.every(function(item, index, array) {
                    return tablesList.indexOf(item) !== -1;
                });
                if (isAllSearchIn) {
                    var result = [];
                    for (var i = 0; i < kvArr.length; i++) {
                        result.push(readValByCol(kvArr[i]));

                    }
                    var isEnd = result.some(function(item, index, array) {
                        return item.indexOf(undefined) !== -1;
                    });
                    if (isEnd) {
                        console.log('查询字段名不存在，无法完成查询操作！');
                    } else {
                        var headers = rv;
                        var rows = cartesianProductOf(...result);
                        console.info();
                        console.info('>>>>查询语句: ' + sql.replace(/\s+/g, ' '));
                        console.info();
                        console.info('>>>>结果数量: ' + rows.length);
                        console.info();
                        console.log('>>>>查询结果: ');
                        console.info();
                        pt = new PrettyTable();
                        pt.create(headers, rows);
                        pt.print();
                        console.timeEnd('>>>>查询耗时: ');
                    }

                    function readValByCol(arr) {
                        var tableName = arr[0];
                        var colName = arr[1];
                        var filePath = `data/${tableName}.json`;
                        var encoding = "utf-8";
                        if (fs.existsSync(filePath)) {
                            var chunk = fs.readFileSync(filePath, encoding);
                            var dataArr = JSON.parse(chunk).data;
                            var col = dataArr[0];
                            dataArr.shift();
                            var data = dataArr;
                            var res = [];
                            for (var i = 0; i < data.length; i++) {
                                res.push(data[i][colName]);
                            }
                            return res;
                        }
                    }

                    function cartesianProductOf() {
                        return Array.prototype.reduce.call(arguments, function(a, b) {
                            var ret = [];
                            a.forEach(function(a) {
                                b.forEach(function(b) {
                                    ret.push(a.concat([b]));
                                });
                            });
                            return ret;
                        }, [
                            []
                        ]);
                    }
                } else {
                    console.log('查询字段的关系名不存在，无法完成查询操作！');
                }

            } else {
                console.log('关系不存在，无法完成查询操作！');
            }
        });
    } else if (reg7.test(sql)) {
        var rs = sql.match(reg7);
        var tableNameArr = rs[1].replace(/\s/gi, '').split(',');
        var experArr = rs[3].replace(/\s/gi, '').split('=');
        var rs = fs.createReadStream('data/tablesList.txt');
        rs.setEncoding('utf8');
        rs.on('data', function(chunk) {
            tablesList = chunk.split(',');
            var isAllIn = tableNameArr.every(function(item, index, array) {
                return tablesList.indexOf(item) !== -1;
            });

            var searchArr = [];
            experArr.forEach(function(item, index, array) {
                searchArr.push(item.split('.'));
            });

            var isAllSearchIn = searchArr.every(function(item, index, array) {
                return read(item[0], 'col').indexOf(item[1]) !== -1;
            });

            if (isAllIn && isAllSearchIn) {
                console.time('>>>>查询耗时: ');
                selectAllFromMulByLink(tableNameArr, experArr);
                console.timeEnd('>>>>查询耗时: ');
            } else {
                console.log('关系或条件不存在，无法完成查询操作！');
            }
        });
    } else if (reg8.test(sql)) {
        var rs = sql.match(reg8);
        var tableNameArr = rs[5].replace(/\s/gi, '').split(',');
        var experArr = rs[7].replace(/\s/gi, '').split('=');
        var colArr = rs[1].split(',');
        var rs = fs.createReadStream('data/tablesList.txt');
        rs.setEncoding('utf8');
        rs.on('data', function(chunk) {
            tablesList = chunk.split(',');
            var isAllIn = tableNameArr.every(function(item, index, array) {
                return tablesList.indexOf(item) !== -1;
            });

            var searchArr = [];
            experArr.forEach(function(item, index, array) {
                searchArr.push(item.split('.'));
            });

            var isAllSearchIn = searchArr.every(function(item, index, array) {
                return read(item[0], 'col').indexOf(item[1]) !== -1;
            });

            if (isAllIn && isAllSearchIn) {
                console.time('>>>>查询耗时: ');
                selectFieldsFromMulByLink(tableNameArr, experArr, colArr);
                console.timeEnd('>>>>查询耗时: ');
            } else {
                console.log('关系或条件不存在，无法完成查询操作！');
            }



        });
    } else if (reg9.test(sql)) {
        //SELECTS * FROM S,SC WHERE S.S#=SC.S# AND S.SNAME=LISI
        console.log(sql);
        var rs = sql.match(reg9);
        var tableNameArr = rs[1].replace(/\s/gi, '').split(',');
        var experStr = rs[3].replace(/\s/gi, '').split('AND');
        var experArr = experStr[0].split('=');
        experStr.shift();
        var experArrs = experStr;

        var rs = fs.createReadStream('data/tablesList.txt');
        rs.setEncoding('utf8');
        rs.on('data', function(chunk) {
            tablesList = chunk.split(',');
            var isAllIn = tableNameArr.every(function(item, index, array) {
                return tablesList.indexOf(item) !== -1;
            });

            var searchArr = [];
            experArr.forEach(function(item, index, array) {
                searchArr.push(item.split('.'));
            });

            var isAllSearchIn = searchArr.every(function(item, index, array) {
                return read(item[0], 'col').indexOf(item[1]) !== -1;
            });

            if (isAllIn && isAllSearchIn) {
                console.time('>>>>查询耗时: ');
                selectAllFromMulByLinkAndExp(tableNameArr, experArr, experArrs);
                console.timeEnd('>>>>查询耗时: ');
            } else {
                console.log('关系或条件不存在，无法完成查询操作！');
            }
        });
    } else if (reg3_index.test(sql)) {
        var sql = sql.replace(/\@index/gi, '');
        var rs = sql.match(reg3);
        var tableName = rs[1];
        var exper = rs[2].replace(/\s/gi, '');
        var dataPos = selectDataPosByIndex(tableName, exper);
        console.time(error('>>>>建立索引查询耗时'));
        var res = selectDataByIndex(tableName, dataPos);
        console.timeEnd(error('>>>>建立索引查询耗时')); 
        var headers = Object.keys(res[0]); 
        var rows = [];
        res.forEach(function(item, index, array) {
            rows.push(es7_values(item));
        });
        console.log('>>>>查询结果: ');
        pt = new PrettyTable();
        pt.create(headers, rows);
        pt.print();
    } else {
        console.log('格式不正确');
    }

    function selectAll(tableName) {
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
            console.info('>>>>查询语句: ' + sql.replace(/\s+/g, ' '));
            console.info();
            console.info('>>>>结果数量: ' + rows.length);
            console.info();
            console.log('>>>>查询结果: ');
            console.info();
            pt = new PrettyTable();
            pt.create(headers, rows);
            pt.print();
        });
    }

    function selectFields(tableName, keyArr) {
        var rs = fs.createReadStream(`data/${tableName}.json`);
        rs.setEncoding('utf8');
        rs.on('data', function(chunk) {
            var colObj = JSON.parse(chunk).data[0];
            var dataArr = JSON.parse(chunk).data;
            var key = Object.keys(colObj);
            var flag = true;
            var headers = keyArr;
            var rows = [];
            keyArr.map(function(item, index, array) {
                if (key.indexOf(item) === -1) {
                    flag = false;
                    console.log('字段' + item + '不存在，无法完成查询操作！');
                }
            });
            if (flag) {
                for (var i = 1; i < dataArr.length; i++) {
                    var arr = [];
                    for (var j = 0; j < keyArr.length; j++) {
                        arr.push(dataArr[i][keyArr[j]])
                    }
                    rows.push(arr);
                }
                console.info();
                console.info('>>>>查询语句: ' + sql.replace(/\s+/g, ' '));
                console.info();
                console.info('>>>>结果数量: ' + rows.length);
                console.info();
                console.log('>>>>查询结果: ');
                console.info();
                pt = new PrettyTable();
                pt.create(headers, rows);
                pt.print();
            }
        });
    }

    function selectAllByMulExp(tableName, experArr) {
        var experObj = {};
        for (var i = 0; i < experArr.length; i++) {
            var arr = experArr[i].split('=');
            experObj[arr[0]] = arr[1];
        }
        var experObjKeys = Object.keys(experObj);
        var experObjValues = es7_values(experObj);
        var rs = fs.createReadStream(`data/${tableName}.json`);
        rs.setEncoding('utf8');
        rs.on('data', function(chunk) {
            var tableColArr = Object.keys(JSON.parse(chunk).data[0]);
            var dataArr = JSON.parse(chunk).data;

            var isAllIn = experObjKeys.every(function(item, index, array) {
                return tableColArr.indexOf(item) !== -1;
            });

            function isContained(a, b) {
                if (!(a instanceof Array) || !(b instanceof Array)) return false;
                if (a.length < b.length) return false;
                var aStr = a.toString();
                for (var i = 0, len = b.length; i < len; i++) {
                    if (aStr.indexOf(b[i]) == -1) return false;
                }
                return true;
            }

            if (isAllIn) {
                var res = [];
                for (var i = 1; i < dataArr.length; i++) {
                    res.push(es7_values(dataArr[i]));
                }
                var rows = [];
                var bFlag = false;
                for (var j = 0; j < res.length; j++) {
                    if (isContained(res[j], experObjValues)) {
                        // console.log(res[j]);
                        rows.push(res[j]);
                        bFlag = true;
                    }
                }

                if (bFlag) {
                    var headers = tableColArr;
                    console.info();
                    console.info('>>>>查询语句: ' + sql.replace(/\s+/g, ' '));
                    console.info();
                    console.info('>>>>结果数量: ' + rows.length);
                    console.info();
                    console.log('>>>>查询结果: ');
                    console.info();
                    pt = new PrettyTable();
                    pt.create(headers, rows);
                    pt.print();
                } else {
                    console.log('查询无结果');
                }
            } else {
                console.log('条件不存在，无法完成查询操作！');
            }
        });
    }

    function selectFieldsByMulExp(tableName, colNameArr, experArr) {
        var experObj = {};
        for (var i = 0; i < experArr.length; i++) {
            var arr = experArr[i].split('=');
            experObj[arr[0]] = arr[1];
        }
        var experObjKeys = Object.keys(experObj);
        var experObjValues = es7_values(experObj);
        var rs = fs.createReadStream(`data/${tableName}.json`);
        rs.setEncoding('utf8');
        rs.on('data', function(chunk) {
            var tableColArr = Object.keys(JSON.parse(chunk).data[0]);
            var dataArr = JSON.parse(chunk).data;

            var isAllIn = experObjKeys.every(function(item, index, array) {
                return tableColArr.indexOf(item) !== -1;
            });

            var isIn = colNameArr.every(function(item, index, array) {
                return tableColArr.indexOf(item) !== -1;
            });



            function isContained(a, b) {
                if (!(a instanceof Array) || !(b instanceof Array)) return false;
                if (a.length < b.length) return false;
                var aStr = a.toString();
                for (var i = 0, len = b.length; i < len; i++) {
                    if (aStr.indexOf(b[i]) == -1) return false;
                }
                return true;
            }
            var pos = [];
            if (isIn) {
                colNameArr.forEach(function(item, index, array) {
                    pos.push(tableColArr.indexOf(item));
                });
            }
            if (isAllIn && isIn) {
                var res = [];
                var keyArr = tableColArr;
                for (var i = 1; i < dataArr.length; i++) {
                    res.push(es7_values(dataArr[i]));
                }
                var results = [];
                var bFlag = false;
                for (var j = 0; j < res.length; j++) {
                    var obj = {};
                    if (isContained(res[j], experObjValues)) {
                        results.push(res[j]);
                        bFlag = true;
                    }
                }
                if (bFlag) {
                    var rows = [];
                    var headers = colNameArr;
                    for (var i = 0; i < results.length; i++) {
                        var arr = [];
                        for (var j = 0; j < pos.length; j++) {
                            arr.push(results[i][pos[j]]);
                        }
                        rows.push(arr);
                    }
                    console.info();
                    console.info('>>>>查询语句: ' + sql.replace(/\s+/g, ' '));
                    console.info();
                    console.info('>>>>结果数量: ' + rows.length);
                    console.info();
                    console.log('>>>>查询结果: ');
                    console.info();
                    pt = new PrettyTable();
                    pt.create(headers, rows);
                    pt.print();
                } else {
                    console.log('查询无结果');
                }
            } else {
                console.log('查询条件或查询字段不存在，无法完成查询操作！');
            }

        });
    }

    function selectAllByMulTable(tableNameArr) {

        function read(tableName) {
            var filePath = `data/${tableName}.json`;
            var encoding = "utf-8";
            if (fs.existsSync(filePath)) {
                var chunk = fs.readFileSync(filePath, encoding);
                var data = JSON.parse(chunk).data;
                data.shift();
                return data;
            }
        }

        var res = [];
        tableNameArr.map(function(item, index, array) {
            res.push(read(item));
        });

        function cartesianProductOf() {
            return Array.prototype.reduce.call(arguments, function(a, b) {
                var ret = [];
                a.forEach(function(a) {
                    b.forEach(function(b) {
                        ret.push(a.concat([b]));
                    });
                });
                return ret;
            }, [
                []
            ]);
        }


        var unionArr = cartesianProductOf(...res);
        var key = unionArr[0];
        var keyArr = [];
        for (var i = 0; i < key.length; i++) {
            for (var k in key[i]) {
                keyArr.push(k);
            }
        }

        var val = [];
        for (var i = 0; i < unionArr.length; i++) {
            var temp = [];
            for (var j = 0; j < unionArr[i].length; j++) {
                temp.push(es7_values(unionArr[i][j]));
            }
            //扁平化多维数组
            val.push(Array.prototype.concat.apply([], temp));
        }
        if (val.length === 0) {
            console.info();
            console.error('查询无结果!');
        } else {
            var headers = keyArr;
            var rows = val;
            console.info();
            console.info('>>>>查询语句: ' + sql.replace(/\s+/g, ' '));
            console.info();
            console.info('>>>>结果数量: ' + rows.length);
            console.info();
            console.log('>>>>查询结果: ');
            console.info();
            pt = new PrettyTable();
            pt.create(headers, rows);
            pt.print();
        }
    }

    function selectAllFromMulByLink(tableNameArr, experArr) {
        var td1 = read(tableNameArr[0], 'data');
        var td2 = read(tableNameArr[1], 'data');
        var tk1 = read(tableNameArr[0], 'col');
        var tk2 = read(tableNameArr[1], 'col');
        var arr = [];
        experArr.forEach(function(item, index, array) {
            arr.push(item.split('.'));
        });
        var k = arr[0][1];
        var k1 = arr[1][1];
        if (k !== k1) {
            console.log('查询条件不存在!');
        } else {
            var res = [];
            td1.forEach(function(item1, index1, array1) {
                td2.forEach(function(item2, index2, array2) {
                    if (item1[k] === item2[k]) {
                        res.push(es7_values(item1).concat(es7_values(item2)));
                    }
                });
            });
            var headers = [].concat(tk1, tk2);
            if (res.length === 0) {
                console.log('查询无结果！');
            } else {
                var rows = res;
                console.info();
                console.info('>>>>查询语句: ' + sql.replace(/\s+/g, ' '));
                console.info();
                console.info('>>>>结果数量: ' + rows.length);
                console.info();
                console.log('>>>>查询结果: ');
                console.info();
                pt = new PrettyTable();
                pt.create(headers, rows);
                pt.print();
            }

        }
    }

    function selectFieldsFromMulByLink(tableNameArr, experArr, colArr) {
        var tables = [];
        var keys = [];
        var pos = [];
        var l = read(tableNameArr[0], 'col').length;

        function getPos(arr) {
            var pos = read(arr[0], 'col').indexOf(arr[1]);
            var s = arr[0];
            var a = [];
            a[0] = s;
            a[1] = pos;
            return a;
        }
        colArr.forEach(function(item, index, array) {
            var arr = item.split('.');
            pos.push(getPos(arr));
            tables.push(arr[0]);
            keys.push(arr[1]);
        });
        var isNot = pos.some(function(item, index, array) {
            return item.indexOf(-1) !== -1;
        });
        var t1 = tableNameArr[0];
        var t2 = tableNameArr[1];
        var a1 = [t1];
        var a2 = [t2];
        pos.forEach(function(item, index, array) {
            if (item[0] === t1) {
                a1.push(item[1]);
            }
            if (item[0] === t2) {
                a2.push(item[1] + l);
            }
        });
        a1.shift();
        a2.shift();
        var resPos = a1.concat(a2);
        var arr = [];
        experArr.forEach(function(item, index, array) {
            arr.push(item.split('.'));
        });
        var k = arr[0][1];
        var td1 = read(tableNameArr[0], 'data');
        var td2 = read(tableNameArr[1], 'data');
        var res = [];
        td1.forEach(function(item1, index1, array1) {
            td2.forEach(function(item2, index2, array2) {
                if (item1[k] === item2[k]) {
                    res.push(es7_values(item1).concat(es7_values(item2)));
                }
            });
        });
        var rr = [];
        res.forEach(function(item, index, array) {
            var a = [];
            resPos.forEach(function(item1, index1, array1) {
                a.push(item[item1]);
            });
            rr.push(a);
        });

        if (!isNot) {
            if (rr.length === 0) {
                console.log('查询无结果！');
            } else {
                var headers = [];
                colArr.forEach(function(item, index, array) {
                    headers.push(item.replace(/\w+\.+/, ''));
                });
                var rows = rr;
                console.info();
                console.info('>>>>查询语句: ' + sql.replace(/\s+/g, ' '));
                console.info();
                console.info('>>>>结果数量: ' + rows.length);
                console.info();
                console.log('>>>>查询结果: ');
                console.info();
                pt = new PrettyTable();
                pt.create(headers, rows);
                pt.print();
            }
        } else {
            console.log('查询字段不存在，无法完成查询操作！');
        }
    }

    function selectAllFromMulByLinkAndExp(tableNameArr, experArr, experArrs) {

        var resExp = [];
        experArrs.forEach(function(item, index, array) {
            resExp.push(item.split('=')[1]);
        });

        function isContained(a, b) {
            if (!(a instanceof Array) || !(b instanceof Array)) return false;
            if (a.length < b.length) return false;
            var aStr = a.toString();
            for (var i = 0, len = b.length; i < len; i++) {
                if (aStr.indexOf(b[i]) == -1) return false;
            }
            return true;
        }



        var td1 = read(tableNameArr[0], 'data');
        var td2 = read(tableNameArr[1], 'data');
        var tk1 = read(tableNameArr[0], 'col');
        var tk2 = read(tableNameArr[1], 'col');
        var arr = [];
        experArr.forEach(function(item, index, array) {
            arr.push(item.split('.'));
        });
        var k = arr[0][1];
        var k1 = arr[1][1];
        if (k !== k1) {
            console.log('查询条件不存在!');
        } else {
            var res = [];
            td1.forEach(function(item1, index1, array1) {
                td2.forEach(function(item2, index2, array2) {
                    if (item1[k] === item2[k]) {
                        res.push(es7_values(item1).concat(es7_values(item2)));
                    }
                });
            });

            if (res.length === 0) {
                console.log('查询无结果！');
            } else {
                // console.log(res);
                var rr = [];
                for (var i = 0; i < res.length; i++) {
                    if (isContained(res[i], resExp)) {
                        rr.push(res[i]);
                    }
                }

                if (rr.length === 0) {
                    console.log('查询无结果！');
                } else {
                    var headers = [].concat(tk1, tk2);
                    var rows = rr;
                    console.info();
                    console.info('>>>>查询语句: ' + sql.replace(/\s+/g, ' '));
                    console.info();
                    console.info('>>>>结果数量: ' + rows.length);
                    console.info();
                    console.log('>>>>查询结果: ');
                    console.info();
                    pt = new PrettyTable();
                    pt.create(headers, rows);
                    pt.print();
                }
            }
        }
    }


    // 读关系文件
    function read(tableName, type) {
        var filePath = `data/${tableName}.json`;
        var encoding = "utf-8";
        if (fs.existsSync(filePath)) {
            var chunk = fs.readFileSync(filePath, encoding);
            var res = JSON.parse(chunk);
            if (type === 'data') {
                var data = JSON.parse(chunk).data;
                data.shift();
                return data;
            } else if (type === 'col') {
                var colObj = JSON.parse(chunk).data[0];
                var cols = Object.keys(colObj);
                return cols;
            }

        }
    }

    // 按照索引查询位置
    function selectDataPosByIndex(tableName, exper) {
        // select@index * from s where sname=liu
        var key = exper.split('=')[0];
        var value = exper.split('=')[1];
        var indexPath = `index/${tableName}_${key}_INDEX.json`;
        var encoding = "utf-8";
        if (fs.existsSync(indexPath)) {
            var chunk = fs.readFileSync(indexPath, encoding);
            var resArr = JSON.parse(chunk).item;
            var resPos = [];
            resArr.forEach(function(item, index, array) {
                if (value === item.value) {
                    resPos.push(parseInt(item.pos) - 1);
                }
            });

        }
        return resPos;
    }

    // 按照索引查询
    function selectDataByIndex(tableName, dataPos) {
        var data = read(tableName, 'data');
        var res = [];
        for (var i = 0; i < dataPos.length; i++) {
            res.push(data[dataPos[i]]);
        }
        return res;
    }

};



/**
 * dropTable
 * @fn    删除关系
 * @sql   DROP TABLE <关系名>
 * @demo  DROP TABLE STUDENTS
 */

DB.dropTable = function() {
    var tableName = this.sqlStr.substring(11);
    var user = this.user;
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

                var dictPath = `./dict/${tableName}.dict`;

                fs.unlink(dictPath, function() {
                    // 存在用户
                    if (user) {
                        //删除关系表中的权限
                        var rs = fs.createReadStream('data/usersList.json');
                        rs.setEncoding('utf8');
                        rs.on('data', function(chunk) {
                            var usersList = JSON.parse(chunk);
                            var arr = [];
                            for (var i = 0; i < usersList.length; i++) {
                                arr.push(usersList[i]['username']);
                            }
                            if (arr.indexOf(user) !== -1) {
                                var usersObj = {};
                                for (var i = 0; i < usersList.length; i++) {
                                    if (usersList[i]['username'] === user) {
                                        usersObj = usersList[i];
                                    }
                                }
                                var powersObj = usersObj.powers;
                                // drop操作的同时将该用户对该表的权限删除
                                delete powersObj[tableName];
                                usersObj['powers'] = powersObj;
                                //	写入文件
                                usersList[usersList.indexOf(usersObj)] = usersObj;
                                var ws = fs.createWriteStream('data/usersList.json');
                                ws.write(JSON.stringify(usersList, null, 2));
                            }
                        });
                        //删除关系表中的权限
                    }
                });


            });
        } else {
            console.log(notice(tableName) + error('不存在！无法删除该关系！'));
        }
    });
};



/**
 * alterTable
 * @fn    增加|删除 属性
 * @sql   ALTER TABLE <关系名> ADD|DROP <列名> <列类型>
 * @demo  DROP TABLE STUDENTS
 */

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



/**
 * deleteTable
 * @fn    删除元组
 * @sql   DELETE FROM <关系名> [WHERE <条件表达式>]
 * @demo  DELETE FROM STUDENTS WHERE NO=220011
 */

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
                var length = oriData.length;
                var res = [];
                for (var item of oriData) {
                    for (var k in item) {
                        if (k === colKey && item[colKey] === colVal) {
                            count++;
                            // data.splice(oriData.indexOf(item));
                            res.push(item);
                            bFlag = true;
                        }
                    }
                }
                if (!bFlag) {
                    console.log(error('该条件 ' + notice(arr[4]) + error(' 无法查询出结果！无法进行删除操作！')));
                } else {
                    res0 && res.unshift(res0);
                    table.name = name;
                    table.data = res;
                    console.log(error('删除 ' + (length - count) + ' 条数据!'));
                }

            }

            // console.log(data);
            // table.name = name;
            // table.data = data;
            var ws = fs.createWriteStream(`data/${tableName}.json`);
            ws.write(JSON.stringify(table, null, 2));

        });
    }

    delItemToFile();
}



/**
 * updateTable
 * @fn    更新元组
 * @sql   UPDATE <关系名> SET <列名>=<常值>,...,<列名>=<常值> [WHERE <条件表达式>]
 * @demo  UPDATE STUDENTS SET AGE=23 WHERE NAME=LIHUA
 */

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


/**
 * createView
 * @fn    创建视图
 * @sql   CREATE VIEW <视图名> [(<列名>,...,<列名>)] AS <SELECT 语句>
 * @demo  CREATE VIEW STUDENTS_VIEW AS SELECT * FROM STUDENTS
 */

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


/**
 * dropView
 * @fn    删除视图
 * @sql   DROP VIEW 视图名
 * @demo  DROP VIEW STUDENTS_VIEW
 */

DB.dropView = function() {
    var arr = this.sqlStr.split(' ');
    var viewName = arr[2];
    var path = `./view/${viewName}.view`;
    fs.unlink(path, function() {
        console.log(error('删除视图 ' + viewName + ' 成功！'));
    });
}

/**
 * showTables
 * @fn    查看所有关系
 * @sql   SHOW TABLES
 * @demo  SHOW TABLES
 */

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



/**
 * createUser
 * @fn    创建用户
 * @sql   CREATE USER 用户名 IDENTIFIED BY 密码
 * @demo  CREATE USER LISI IDENTIFIED BY LISI123456
 */

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
                //
                var rs = fs.createReadStream('data/tablesList.txt');
                rs.setEncoding('utf8');
                rs.on('data', function(chunk) {
                    var tablesList = chunk.split(',');
                    info.powers = {};
                    for (var i = 0; i < tablesList.length; i++) {
                        info.powers[tablesList[i]] = [];
                    }
                    infos.push(info);
                    var ws = fs.createWriteStream('data/usersList.json');
                    ws.write(JSON.stringify(infos, null, 2));
                    console.log(error('用户 ' + username + ' 创建成功！'));
                });

            }

        });
    }
    createUser();
}


/**
 * grantPower
 * @fn        赋予权限
 * @sql       GRANT <权限表> ON <对象> TO <用户ID表> [WITH GRANT OPTION]
 * @demo      GRANT UPDATE ON STUDENTS TO LISI
 * @<权限表>  ['CREATE', 'DROP', 'ALTER', 'DELETE', 'UPDATE', 'SELECT', 'SHOW', 'GRANT', 'REVOKE', 'INSERT', 'VIEW']
 * @<对象>     关系名
 */

var grantPower = function(sql) {

    var arr = sql.toUpperCase().split(' ');
    var fn = arr[1];
    var tableName = arr[3];
    var username = arr[5];

    grantPowerAndTableToUser(tableName, username);


    function grantPowerAndTableToUser(table, user) {
        var rs = fs.createReadStream('data/usersList.json');
        rs.setEncoding('utf8');
        rs.on('data', function(chunk) {

            var usersList = JSON.parse(chunk);
            var arr = [];
            for (var i = 0; i < usersList.length; i++) {
                arr.push(usersList[i]['username']);
            }
            if (arr.indexOf(username) === -1) {
                console.info();
                console.info();
                console.log(error('用户 ' + notice(username) + error(' 不存在！无法完成grant操作!')));
            } else {
                if (username === 'ROOT' || username === 'ADMIN') {
                    console.info();
                    console.info();
                    console.log(error('\nwarning: 根用户或者管理员用户的权限无法修改!\n'));
                } else {
                    var usersObj = {};
                    for (var i = 0; i < usersList.length; i++) {
                        if (usersList[i]['username'] === username) {
                            usersObj = usersList[i];
                        }
                    }
                    var powersObj = usersObj.powers;
                    var powersTableArr = Object.keys(powersObj);
                    if (powersTableArr.indexOf(tableName) !== -1) {
                        var powersList = ['CREATE', 'DROP', 'ALTER', 'DELETE', 'UPDATE', 'SELECT', 'SHOW', 'GRANT', 'REVOKE', 'INSERT', 'VIEW'];
                        if (powersList.indexOf(fn) === -1) {
                            console.info();
                            console.info();
                            console.log(error('输入的权限关键字 ') + notice(fn) + error(' 不存在! 无法完成grant操作! \n权限关键字有：CREATE,DROP,ALTER,DELETE,UPDATE,SELECT,SHOW,GRANT,REVOKE,INSERT', 'VIEW'));
                        } else {
                            var powersInTable = powersObj[powersTableArr[powersTableArr.indexOf(tableName)]];
                            if (powersInTable.indexOf(fn) === -1) {
                                //	进行添加操作
                                powersInTable.push(fn);
                                powersObj[tableName] = powersInTable;
                                usersObj['powers'] = powersObj;
                                //	写入文件
                                usersList[usersList.indexOf(usersObj)] = usersObj;
                                var ws = fs.createWriteStream('data/usersList.json');
                                ws.write(JSON.stringify(usersList, null, 2));
                                console.info();
                                console.info();
                                console.log(('关系' + tableName + '的' + fn + '权限赋给' + '用户' + username) + error(' 注：权限赋予后重新登录才能生效！'));
                            } else {
                                console.info();
                                console.info();
                                console.log(error('权限 ') + notice(fn) + error(' 已经被赋予！无需重复进行grant操作!'));
                            }
                        }
                    } else {
                        var rs = fs.createReadStream('data/tablesList.txt');
                        rs.setEncoding('utf8');
                        rs.on('data', function(chunk) {
                            var tablesList = chunk.split(',');
                            if (tablesList.indexOf(tableName) !== -1) {
                                var powersObj = usersObj.powers;
                                powersObj[tableName] = [];
                                var powersList = ['CREATE', 'DROP', 'ALTER', 'DELETE', 'UPDATE', 'SELECT', 'SHOW', 'GRANT', 'REVOKE', 'INSERT', 'VIEW'];
                                if (powersList.indexOf(fn) === -1) {
                                    console.info();
                                    console.info();
                                    console.log(error('输入的权限关键字 ') + notice(fn) + error(' 不存在! 无法完成grant操作! \n权限关键字有：CREATE,DROP,ALTER,DELETE,UPDATE,SELECT,SHOW,GRANT,REVOKE,INSERT,VIEW'));
                                } else {

                                    //	进行添加操作
                                    powersObj[tableName].push(fn)
                                        // console.log(powersObj);
                                    usersObj['powers'] = powersObj;

                                    //	写入文件
                                    usersList[usersList.indexOf(usersObj)] = usersObj;
                                    var ws = fs.createWriteStream('data/usersList.json');
                                    ws.write(JSON.stringify(usersList, null, 2));
                                    console.info();
                                    console.info();
                                    console.log(('关系' + tableName + '的' + fn + '权限赋给' + '用户' + username) + error(' 注：权限赋予后重新登录才能生效！'));
                                }
                            } else {
                                console.info();
                                console.info();
                                console.log(error('关系 ') + notice(tableName) + error(' 不存在！无法完成grant操作!'));
                            }
                        });


                    }
                }

            }

        });

    }

};


/**
 * revoke
 * @fn        撤销权限
 * @sql       REVOKE <权限表> ON <对象> FROM <用户ID表> [WITH REVOKE OPTION]
 * @demo      REVOKE INSERT ON STUDENTS FROM LISI
 * @<权限表>  ['CREATE', 'DROP', 'ALTER', 'DELETE', 'UPDATE', 'SELECT', 'SHOW', 'GRANT', 'REVOKE', 'INSERT', 'VIEW']
 * @<对象>     关系名
 */

DB.revoke = function() {
    var arr = this.sqlStr.split(' ');
    var fn = arr[1];
    var tableName = arr[3];
    var username = arr[5];

    revokePowerAndTableToUser(tableName, username);

    function revokePowerAndTableToUser(table, user) {
        var rs = fs.createReadStream('data/usersList.json');
        rs.setEncoding('utf8');
        rs.on('data', function(chunk) {
            var usersList = JSON.parse(chunk);
            var arr = [];
            for (var i = 0; i < usersList.length; i++) {
                arr.push(usersList[i]['username']);
            }
            if (arr.indexOf(username) === -1) {
                console.info();
                console.info();
                console.log(error('用户 ' + notice(username) + error(' 不存在！无法完成revoke操作!')));
            } else {
                if (username === 'ROOT' || username === 'ADMIN') {
                    console.info();
                    console.info();
                    console.log(error('\nwarning: 根用户或者管理员用户的权限无法撤销!\n'));
                } else {
                    var usersObj = {};
                    for (var i = 0; i < usersList.length; i++) {
                        if (usersList[i]['username'] === username) {
                            usersObj = usersList[i];
                        }
                    }
                    var powersObj = usersObj.powers;
                    var powersTableArr = Object.keys(powersObj);
                    if (powersTableArr.indexOf(tableName) !== -1) {
                        var powersList = ['CREATE', 'DROP', 'ALTER', 'DELETE', 'UPDATE', 'SELECT', 'SHOW', 'GRANT', 'REVOKE', 'INSERT', 'VIEW'];
                        if (powersList.indexOf(fn) === -1) {
                            console.info();
                            console.info();
                            console.log(error('输入的权限关键字 ') + notice(fn) + error(' 不存在! 无法完成revoke操作! \n权限关键字有：CREATE,DROP,ALTER,DELETE,UPDATE,SELECT,SHOW,GRANT,REVOKE,INSERT,VIEW'));
                        } else {
                            var powersInTable = powersObj[powersTableArr[powersTableArr.indexOf(tableName)]];
                            if (powersInTable.indexOf(fn) === -1) {
                                console.info();
                                console.info();
                                console.log(error('权限 ') + notice(fn) + error(' 未被赋予！无法完成revoke操作!'));
                            } else {
                                // console.log('revoke 操作');
                                powersInTable.splice(powersInTable.indexOf(fn), 1);

                                powersObj[tableName] = powersInTable;
                                usersObj['powers'] = powersObj;
                                usersList[usersList.indexOf(usersObj)] = usersObj;
                                //	写入文件
                                var ws = fs.createWriteStream('data/usersList.json');
                                ws.write(JSON.stringify(usersList, null, 2));
                                console.info();
                                console.info();
                                console.log('撤销用户' + username + '对关系' + tableName + '的' + fn + '权限');
                            }
                        }

                    } else {
                        console.info();
                        console.info();
                        console.log(error('关系 ') + notice(tableName) + error(' 不存在！无法完成grant操作!'));
                    }
                }

            };

        });
    }

}


/**
 * createIndex
 * @fn    创建索引
 * @sql   CREATE [UNIQUE] INDEX <索引名> ON <关系名> (<列名> [ORDER],...,<列名> [ORDER]) [CLUSTER]
 * @demo  CREATE INDEX INDEX_NAME ON STUDENTS (NO DESC|ASC|NULL)
 * @order DESC:降序  ASC:升序  默认(NULL):ASC
 */

DB.createIndex = function() {
    var arr = this.sqlStr.split(' ');
    var indexName = arr[2];
    var tableName = arr[4];
    var keyAndType = this.sqlStr.substring(this.sqlStr.indexOf("(") + 1, this.sqlStr.indexOf(")")).replace(/'/g, "").split(' ');
    var key = keyAndType[0];
    var type = "";
    if (keyAndType[1] === "DESC") {
        type = "desc";
    } else if (keyAndType[1] === "ASC") {
        type = "asc";
    } else {
        type = "asc";
    }

    function createIndex() {
        var rs = fs.createReadStream('data/tablesList.txt');
        rs.setEncoding('utf8');
        rs.on('data', function(chunk) {
            tablesList = chunk.split(',');
            if (tablesList.indexOf(tableName) === -1) {
                console.log('关系 ' + notice(tableName) + error(' 不存在！无法创建索引！'));
            } else {
                var rs = fs.createReadStream(`data/${tableName}.json`);
                rs.setEncoding('utf8');
                rs.on('data', function(chunk) {
                    var keyArr = Object.keys(JSON.parse(chunk).data[0]); 
                    var indexArr =  JSON.parse(chunk).index; 
                    if (keyArr.indexOf(key) === -1) {
                        console.log('字段 ' + notice(key) + error(' 不存在！无法创建索引！'));
                    } else {
                        if (indexArr.indexOf(key) !== -1) {
                            console.log('字段 ' + notice(key) + error(' 已经创建了索引！无需重复创建！'));
                        } else {
                            // console.log('可以创建索引！');
                            var rs = fs.createReadStream(`data/${tableName}.json`);
                            rs.setEncoding('utf8');
                            rs.on('data', function(chunk) {
                                var tableObj = JSON.parse(chunk);
                                var data = JSON.parse(chunk).data;
                                var indexType = data[0][key];
                                var indexObj = {};
                                indexObj['name'] = indexName;
                                indexObj['key'] = key;
                                indexObj['type'] = indexType;
                                indexObj['total'] = data.length - 1;
                                indexObj['table'] = tableName;
                                var indexItem = [];
                                var keyOri = [];
                                for (var i = 1; i < data.length; i++) {
                                    var itemObj = {};
                                    itemObj['value'] = data[i][key];
                                    itemObj['pos'] = i;
                                    indexItem.push(itemObj);
                                }

                                function compare(property, type) {
                                    return function(a, b) {
                                        var value1 = a[property];
                                        var value2 = b[property];
                                        if (type === 'asc') {
                                            return value1 - value2;
                                        } else {
                                            return value2 - value1;
                                        }
                                    }
                                }
                                if (type === 'desc') {
                                    indexItem = indexItem.sort(compare('value', 'desc'));
                                } else {
                                    indexItem = indexItem.sort(compare('value', 'asc'));
                                }
                                indexObj['item'] = indexItem;
                                var ws = fs.createWriteStream(`index/${indexObj['name']}.json`);
                                ws.write(JSON.stringify(indexObj, null, 2));
                                tableObj['index'].push(key);
                                // console.log(tableObj);
                                var ws = fs.createWriteStream(`data/${tableName}.json`);
                                ws.write(JSON.stringify(tableObj, null, 2));
                                console.info();
                                console.log(error('创建索引成功！'));
                            });
                        }
                    }
                });
            }
        });
    }
    createIndex();
}



/**
 * showIndex
 * @fn    显示索引
 * @sql   SHOW INDEX FROM <关系名>
 * @demo  SHOW INDEX FROM STUDENTS
 */

DB.showIndex = function() {
    var self = this;
    var arr = this.sqlStr.split(' ');
    var tableName = arr[3];

    function showIndex() {
        var rs = fs.createReadStream('data/tablesList.txt');
        rs.setEncoding('utf8');
        rs.on('data', function(chunk) {
            tablesList = chunk.split(',');
            if (tablesList.indexOf(tableName) === -1) {
                console.log('关系 ' + notice(tableName) + error(' 不存在！无法显示索引！'));
            } else {
                var rs = fs.createReadStream(`data/${tableName}.json`);
                rs.setEncoding('utf8');
                rs.on('data', function(chunk) {
                    var tableObj = JSON.parse(chunk);
                    var indexArr = tableObj['index'];
                    var indexColl = '';
                    var sep = ', ';
                    for (var i = 0; i < indexArr.length; i++) {
                        if (i === indexArr.length - 1) {
                            sep = '';
                        }
                        indexColl += indexArr[i] + sep;
                    }
                    console.info();
                    console.log(`现关系 ${tableName} 已创建的索引字段有：${indexColl}  \n若需要查看具体索引内容请按照如下格式输入： index@索引名 (eg: index@NO)`);
                    console.info();
                    self.initShowIndexDetail(tableName);
                });
            }
        });
    }
    showIndex();
}

DB.initShowIndexDetail = function(tableName) {
    this.tableName = tableName;
}


/**
 * showIndexDetail
 * @fn    显示索引详情
 * @demo  INDEX@INDEX_NAME
 */

DB.showIndexDetail = function() {
    var arr = this.sqlStr.split('@');
    var indexName = arr[1];
    var tableName = this.tableName;
    var indexFileName = `index/${tableName}_${indexName}_INDEX.json`;
    var rs = fs.createReadStream(indexFileName);
    rs.setEncoding('utf8');
    rs.on('data', function(chunk) {
        var indexObj = JSON.parse(chunk);
        var headers = ['VALUES', 'POS'];
        var rows = [];
        for (var i = 0; i < indexObj.item.length; i++) {
            rows.push(es7_values(indexObj.item[i]));
        }
        console.info();
        console.info();
        console.log(`>>>>>>>>>>索引信息<<<<<<<<<<`);
        console.info();
        console.log(`----------------------------`);
        console.log(`■ 关系名：${indexObj['table']}`);
        console.log(`----------------------------`);
        console.log(`■ 字段名：${indexObj['key']}`);
        console.log(`----------------------------`);
        console.log(`■ 索引类型：${indexObj['type']}`);
        console.log(`----------------------------`);
        console.log(`■ 索引总数：${indexObj['total']}`);
        console.log(`----------------------------`);
        console.info();
        console.log(`>>>>>>>>>>索引详表<<<<<<<<<<`);
        console.info();
        pt = new PrettyTable();
        pt.create(headers, rows);
        // pt.sortTable("POS");
        // pt.sortTable("POS", reverse=true);
        pt.print();
    });

    rs.on('error', function() {
        console.log('输入的索引不存在！');
    });
}



/**
 * dropIndex
 * @fn    删除索引
 * @sql   DROP INDEX <索引名> ON <关系名>
 * @demo  DROP INDEX STUDENTS_AGE_INDEX ON STUDENTS
 */


DB.dropIndex = function() {
    var arr = this.sqlStr.split(' ');
    var indexFileName = arr[2];
    var tableName = arr[4];

    function dropIndex() {
        var rs = fs.createReadStream('data/tablesList.txt');
        rs.setEncoding('utf8');
        rs.on('data', function(chunk) {
            tablesList = chunk.split(',');
            if (tablesList.indexOf(tableName) === -1) {
                console.info();
                console.log(notice(tableName) + error('不存在！无法完成删除索引操作'));
            } else {
                var rs = fs.createReadStream(`index/${indexFileName}.json`);
                rs.setEncoding('utf8');
                rs.on('data', function(chunk) {
                    var indexObj = JSON.parse(chunk);
                    var indexKey = indexObj['key'];
                    var rs = fs.createReadStream(`data/${tableName}.json`);
                    rs.setEncoding('utf8');
                    rs.on('data', function(chunk) {
                        var oriData = JSON.parse(chunk);
                        var tableIndex = JSON.parse(chunk)['index'];
                        if (tableIndex.indexOf(indexKey) !== -1) {
                            var idx = tableIndex.indexOf(indexKey);
                            tableIndex.splice(idx, 1);
                            oriData['index'] = tableIndex;
                            var ws = fs.createWriteStream(`data/${tableName}.json`);
                            ws.write(JSON.stringify(oriData, null, 2));

                            var path = `./index/${indexFileName}.json`;
                            fs.unlink(path, function() {
                                console.info();
                                console.log(error('删除索引 ' + indexFileName + ' 成功！'));
                            });
                        } else {
                            console.info();
                            console.log('索引不存在！');
                        }
                    });

                });
            }
        });
    }
    dropIndex();
}



DB.showDict = function() {
    var tableName = this.sqlStr.split(' ')[1];
    var rs = fs.createReadStream('data/tablesList.txt');
    rs.setEncoding('utf8');
    rs.on('data', function(chunk) {
        var tablesList = chunk.split(',');
        if (tablesList.indexOf(tableName) !== -1) {
            var rs = fs.createReadStream(`dict/${tableName}.dict`);
            rs.setEncoding('utf8');
            rs.on('data', function(chunk) {
                console.info();
                console.info();
                console.log(chunk);
            });

        } else {
            console.info();
            console.log(notice(tableName) + error('不存在！无法查看数据字典文件！'));
        }

    });

}

/**
 * init
 * 初始化
 */

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
        //
        var createIndex = this.sqlStr.startsWith('CREATE INDEX');
        var showIndex = this.sqlStr.startsWith('SHOW INDEX');
        var showIndexDetail = this.sqlStr.startsWith('INDEX@');
        var dropIndex = this.sqlStr.startsWith('DROP INDEX');
        //
        var showDict = this.sqlStr.startsWith('DESCRIBE');
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
        } else if (createIndex) {
            this.createIndex();
        } else if (showIndex) {
            this.showIndex();
        } else if (showIndexDetail) {
            this.showIndexDetail();
        } else if (dropIndex) {
            this.dropIndex();
        } else if (showDict) {
            this.showDict();
        } else {
            console.info();
            console.log(error('输入有误!'));
            console.info();
        }

    } else {


        this.sqlStr = sql.toString().toUpperCase();
        var tables = Object.keys(powers);
        // console.log(tables);
        if (this.sqlStr.startsWith('CREATE TABLE')) {
            var arr = es7_values(powers);
            var isCreate = false;
            for (var i = 0; i < arr.length; i++) {
                for (var j = 0; j < arr[i].length; j++) {
                    if (arr[i][j].indexOf('CREATE') !== -1) {
                        isCreate = true;
                    }
                }
            }
            if (isCreate) {
                this.createTable();
            } else {
                console.info();
                console.log('无 create 权限！');
                console.info();
            }
        }

        if (this.sqlStr.startsWith('DROP TABLE')) {
            var tableName = this.sqlStr.substring(11);
            var powers = powers[tableName];

            if (tables.indexOf(tableName) !== -1 && powers.indexOf('DROP') !== -1) {
                this.dropTable();
            } else {
                console.info();
                console.log('无 drop 权限！');
                console.info();
            }
        }

        if (this.sqlStr.startsWith('ALTER TABLE')) {
            var tableName = this.sqlStr.split(' ')[2];
            var powers = powers[tableName];

            if (tables.indexOf(tableName) !== -1 && powers.indexOf('ALTER') !== -1) {
                this.alterTable();
            } else {
                console.info();
                console.log('无 alter 权限！');
                console.info();
            }
        }

        if (this.sqlStr.startsWith('INSERT INTO')) {
            var tableName = this.sqlStr.substring(12, this.sqlStr.indexOf("(") - 7);
            var powers = powers[tableName];

            if (tables.indexOf(tableName) !== -1 && powers.indexOf('INSERT') !== -1) {
                this.insertTable();
            } else {
                console.info();
                console.log('无 insert 权限！');
                console.info();
            }

        }

        if (this.sqlStr.startsWith('DELETE FROM')) {

            var tableName = this.sqlStr.split(' ')[2];
            var powers = powers[tableName];

            if (tables.indexOf(tableName) !== -1 && powers.indexOf('DELETE') !== -1) {
                this.deleteTable();
            } else {
                console.info();
                console.log('无 delete 权限！');
                console.info();
            }

        }

        if (this.sqlStr.startsWith('UPDATE')) {

            var tableName = this.sqlStr.split(' ')[1];
            var powers = powers[tableName];

            if (tables.indexOf(tableName) !== -1 && powers.indexOf('UPDATE') !== -1) {
                this.updateTable();
            } else {
                console.info();
                console.log('无 update 权限！');
                console.info();
            }

        }

        if (this.sqlStr.startsWith('SELECT')) {

            var tableName = this.sqlStr.substring(14);
            var powers = powers[tableName];

            if (tables.indexOf(tableName) !== -1 && powers.indexOf('SELECT') !== -1) {
                this.selectTable();
            } else {
                console.info();
                console.log('无 select 权限！');
                console.info();
            }

        }

        if (this.sqlStr.startsWith('GRANT')) {

            var arr = es7_values(powers);
            var isGrant = false;
            for (var i = 0; i < arr.length; i++) {
                for (var j = 0; j < arr[i].length; j++) {
                    if (arr[i][j].indexOf('GRANT') !== -1) {
                        isGrant = true;
                    }
                }
            }
            if (isGrant) {
                grantPower(this.sqlStr);
            } else {
                console.info();
                console.log('无 grant 权限！');
                console.info();
            }

        }

        if (this.sqlStr.startsWith('REVOKE')) {

            var arr = es7_values(powers);
            var isRevoke = false;
            for (var i = 0; i < arr.length; i++) {
                for (var j = 0; j < arr[i].length; j++) {
                    if (arr[i][j].indexOf('GRANT') !== -1) {
                        isRevoke = true;
                    }
                }
            }
            if (isRevoke) {
                this.revoke();
            } else {
                console.info();
                console.log('无 revoke 权限！');
                console.info();
            }

        }


        if (this.sqlStr.startsWith('CREATE VIEW')) {
            var arr = this.sqlStr.split(' ');
            var tableName = arr[7];
            var powers = powers[tableName];

            if (tables.indexOf(tableName) !== -1 && powers.indexOf('VIEW') !== -1) {
                this.createView();
            } else {
                console.info();
                console.log('无 create view 权限！');
                console.info();
            }

        }


        if (this.sqlStr.startsWith('DROP VIEW')) {
            var arr = es7_values(powers);
            var isDropView = false;
            for (var i = 0; i < arr.length; i++) {
                for (var j = 0; j < arr[i].length; j++) {
                    if (arr[i][j].indexOf('VIEW') !== -1) {
                        isDropView = true;
                    }
                }
            }
            if (isDropView) {
                this.dropView();
            } else {
                console.info();
                console.log('无 drop view 权限！');
                console.info();
            }
        }

        if (this.sqlStr.startsWith('SHOW TABLES')) {

            var arr = es7_values(powers);
            var isShow = false;
            for (var i = 0; i < arr.length; i++) {
                for (var j = 0; j < arr[i].length; j++) {
                    if (arr[i][j].indexOf('SHOW') !== -1) {
                        isShow = true;
                    }
                }
            }
            if (isShow) {
                this.showTables();
            } else {
                console.info();
                console.log('无 show tables 权限！');
                console.info();
            }

        }

    }

}



/**
 * [power 检查权限]
 * @param  {Boolean} isPower 
 * @param  {[Array]}  powers  
 */

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
                console.log("\n****************************************\n" + "\n·help      ----      帮助信息\n·info      ----      有关信息\n·end       ----      关闭http服务\n·sql       ----      sql语句示例\n·ssql      ----      select查询语句详解示例\n·clear     ----      清屏\n·exit      ----      退出\n" + "\n****************************************\n");
                break;
            case 'sql':
                console.log("\n********************************************************************************************\n" + "\n·创建关系 CREATE TABLE              ----      CREATE TABLE DEMO(NO INT, NAME CHAR, AGE INT)\n·删除关系 DROP TABLE                ----      DROP TABEL DEMO\n·添加|删除列属性 ALTER TABLE        ----      ALTER TABLE DEMO ADD|DROP SEX CHAR\n·插入 INSERT INTO                   ----      INSERT INTO DEMO VALUES(1001,DBNAME,12) \n·删除 DELETE                        ----      DELETE FROM DEMO WHERE NAME=TOM\n·更新 UPDATE                        ----      UPDATE DEMO SET NAME=DBMS WHERE NO=1\n·创建视图 CREATE VIEW               ----      CREATE VIEW DEMO AS SELECT * FROM TABLE\n·删除视图 DROP VIEW                 ----      DROP VIEW DEMO\n·列出所有关系 SHOW TABLES           ----      SHOW TABLES\n·创建用户 CREATE USER               ----      CREATE USER USERNAME IDENTIFIED BY PASSWORD\n·赋予权限 GRANT                     ----      GRANT SELECT ON DEMO TO USERNAME\n·撤销权限 REVOKE                    ----      REVOKE SELECT ON DEMO FROM USERNAME\n·创建索引 CREATE INDEX              ----      CREATE INDEX INDEX_NAME ON DEMO (NO)\n·显示索引 SHOW INDEX                ----      SHOW INDEX FROM DEMO\n·删除索引 DROP INDEX                ----      DROP INDEX INDEX_NAME ON DEMO\n·查看数据字典 DESCRIBE              ----      DESCRIBE DEMO\n" + "\n********************************************************************************************\n");
                break;
            case 'ssql':
                console.log("\n****************************************************************************************************\n" + "\n·全关系选择操作                    ----      select * from s\n·单关系投影操作                    ----      select sname,age from s\n·单关系选择操作                    ----      select * from s where age=19[ and sex=m]\n·单关系选择和投影操作              ----      selects s#,sname[,age] from s where sex=m[ and age=19]\n·多关系笛卡尔积操作                ----      select * from s,c\n·两个关系投影操作                  ----      select s.sname,c.cname from s,c\n·两个关系连接操作                  ----      select * from s,sc where s.s#=sc.s#\n·两个关系连接和投影操作            ----      select s.sname,sc.grade from s,sc where s.s#=sc.s#\n·两个关系选择和连接操作            ----      selects * from s,sc where s.s#=sc.s# and sc.grade=80\n\n**注：[]中内容可选\n" + "\n****************************************************************************************************\n");
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
console.log("请输入用户名和密码（输入格式为：用户名@密码，例如：admin@123456） \n要想使用默认用户(管理员)登录请输入 :root\n");


/**
 * Node REPL
 * 
 */

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
                    if (username === 'ROOT' || username === 'ADMIN') {
                        power(1, 'ALL'); 
                    } else {
                        DB.user = username;
                        var usersObj = {};
                        for (var i = 0; i < infos.length; i++) {
                            if (infos[i]['username'] === username) {
                                usersObj = infos[i];
                            }
                        }

                        var powerListHead = ['#TABLES#', 'CREATE', 'DROP', 'ALTER', 'DELETE', 'UPDATE', 'SELECT', 'SHOW', 'GRANT', 'REVOKE', 'INSERT', 'VIEW'];
                        var powerList = ['CREATE', 'DROP', 'ALTER', 'DELETE', 'UPDATE', 'SELECT', 'SHOW', 'GRANT', 'REVOKE', 'INSERT', 'VIEW'];
                        var rows = [];
                        var v = [];
                        for (var k in usersObj.powers) {
                            var values = usersObj.powers[k];
                            v.push(values);
                        }

                        var y = [];
                        for (var i = 0; i < v.length; i++) {
                            var x = [];
                            for (var j = 0; j < powerList.length; j++) {
                                if (inArray(powerList, v[i][j]) || inArray(powerList, v[i][j]) === 0) {
                                    x[inArray(powerList, v[i][j])] = '√';
                                }
                            }
                            y.push(x);
                        }

                        var tb = Object.keys(usersObj.powers);
                        for (var i = 0; i < tb.length; i++) {
                            y[i].unshift(tb[i]);
                        }

                        for (var i = 0; i < y.length; i++) {
                            for (var j = 0; j < 12; j++) {
                                if (!y[i][j]) {
                                    y[i][j] = "-";
                                }
                            }
                        }


                        function inArray(arr, item) {
                            if (arr) {
                                for (var i = 0; i < arr.length; i++) {
                                    if (arr[i] === item) {
                                        return i;
                                    }
                                }
                                return false;
                            }
                        }

                        console.log('当前用户：' + username);
                        console.info();
                        console.log('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>用户权限一览表<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<');
                        console.info();
                        pt = new PrettyTable();
                        pt.create(powerListHead, y);
                        pt.print();
                        var powers = usersObj.powers;
                        power(1, powers);

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