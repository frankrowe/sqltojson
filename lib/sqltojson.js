var Connection = require('tedious').Connection
  , Request = require('tedious').Request
  , TYPES = require('tedious').TYPES
  , fs = require('fs')
  , path = require('path')

function SqlToJson(config){
  
  if(config.query_path) {
    this.query_path = config.query_path
  } else {
    this.query_path = path.join(__dirname, '/queries')
  }

  if(config.colToRow){
    this.colToRow = config.colToRow
  } else {
    this.colToRow = false
  }

  if(config.conn_config) {
    this.conn_config = config.conn_config
  } else {
    this.conn_config = {}
  }

  if(config.filters){
    this.filters = config.filters
  } else {
    this.filters = false
  }

  //mssql || pgsql
  if(config.dbtype){
    this.dbtype = config.dbtype
  } else {
    this.dbtype = 'mssql'
  }
  this.type_dict = {
    'varchar': TYPES.VarChar,
    'int': TYPES.Int
  }
}

SqlToJson.prototype.loadFile = function(file, parameters, next) {
  var self = this
  fs.readFile(this.query_path + '/' + file, function (err, data) {
    if (err) throw err
    var statement = data.toString().replace(/(\n|\r|\r\n)$/, '')
    if(self.filters) {
      var query_info = self.filters.addFilters(statement, parameters)
    } else {
      var query_info = {
        statement: statement,
        parameters: []
      }
    }
    self.loadStatement(query_info.statement, query_info.parameters, function(data) {
      next(data)
    })
  })
}

SqlToJson.prototype.loadStatement = function(statement, parameters, next) {
  var self = this
  if(this.dbtype === 'mssql') {
    var connection = new Connection(this.conn_config)
    connection.on('connect', function(err) {
      if(err) {
        console.log(err)
      } else {
        statement = statement.toString()
        request = new Request(statement, function(err, rowCount) {
          if (err) {
            console.log(err)
          } else {
            if(self.colToRow) {
              next(rowsToObject(rows))
            } else {
              next(rowsToObjectLiteral(rows))
            }
          }
          connection.close()
        })
        if(parameters && parameters.length) {
          parameters.forEach(function(parameter){
            request.addParameter(parameter.name, self.type_dict[parameter.type], parameter.value);
          })
        }
        var rows = []
        request.on('row', function(columns) {
          rows.push(columns)
        })
        connection.execSql(request)
      }
    })
  }
  if(this.dbtype === 'pgsql') {
    //TODO
  }
}

/*
* {name: colName, value: value}
*/
function rowsToObject(rows) {
  var result = []
  rows.forEach(function(row) {
    var newrow = []
    row.forEach(function(column){
      var obj = {
        name: column.metadata.colName,
        value: column.value
      }
      newrow.push(obj)
    })
    result.push(newrow)
  })
  return result
}

/*
* {colName: value}
*/
function rowsToObjectLiteral(rows) {
  var result = []
  rows.forEach(function(row) {
    var newrow = {}
    row.forEach(function(column){
      newrow[column.metadata.colName] = column.value
    })
    result.push(newrow)
  })
  return result
}

module.exports = SqlToJson