const express = require('express');
const app = express();

//Mysql Part

const mysql = require('mysql');
// const router = require('./router/mysql')

app.use(express.json())
const keys = require('./config/key')
const port = process.env.PORT || 5000;
app.listen(port, ()=>{
  console.log(`Server is running on port ${port}`)
})

// app.use('/api',router)
const connection = mysql.createConnection(keys)
connection.connect((err)=>{
    if(err){
      console.log(err)
    }
    else{
      console.log("connected")
    }
  });

app.get("/mysql/imdb/getMovie",(req, res)=>{
    connection.query("SELECT * FROM `title_basics` LIMIT 20", function(error, results, field){
      if(error) throw error;
      // console.log("solution = ", results)
      res.send(results)
    })
  })
  
  app.post("/mysql/imdb/getTotal",(req, res)=>{
    sql = 'SELECT COUNT(*) as total FROM ('+req.body.sqlQuery + ')t'
    console.log(sql)
    connection.query(sql, function(error, results, field){
      if(error) throw error;
      var response = results[0];
      res.send(response)
    })
  })
  
  app.post("/mysql/imdb/commonSQL",(req, res)=>{
    startTime = new Date().getTime();
    sql = req.body.sqlQuery + ' LIMIT ' + req.body.pageFrom + ',' + req.body.pageSize
    connection.query(sql, function(error, results, field){
      if(error) throw error;
      endTime = new Date().getTime();
      var response = {};
      response['runningTime'] = endTime - startTime;
      response['result'] = results;
      res.send(response)
    })
  })

  app.post("/mysql/imdb/getName",(req, res)=>{
    startTime = new Date().getTime();
    sqlQuery = 'select * from name_basics'
    // console.log(req.body)
    var condition = req.body.condition;
    console.log(condition)
    var whereQuery = '';
    if(Object.keys(condition).length > 0){
      whereQuery = ' WHERE '
      for(let i = 0; i < Object.keys(condition).length; i++){
        let item = Object.keys(condition)[i]
        if(i != Object.keys(condition).length - 1)
          whereQuery += item + " LIKE '%"+ condition[item] + "%' AND "
        else
        whereQuery += item + " LIKE '%"+ condition[item] + "%'"
      }
    }
    sql = sqlQuery + whereQuery + ' LIMIT ' + req.body.pageFrom + ',' + req.body.pageSize
    connection.query(sql, function(error, results, field){
      if(error) throw error;
      endTime = new Date().getTime();
      var response = {};
      response['runningTime'] = endTime - startTime;
      response['result'] = results;
      res.send(response)
    })
  })

  app.post("/mysql/imdb/getNameDetail",(req, res)=>{
    startTime = new Date().getTime();
    sqlQueryTitle = 'SELECT DISTINCT primaryTitle FROM title_basics JOIN name_known_for_title USING(tid)';
    sqlQueryProf = 'SELECT DISTINCT profession FROM name_primary_profession'
    var nid = req.body.nid;
    var whereQuery = " WHERE nid = '" +nid+ "'";
    var response = {};
    connection.query(sqlQueryTitle + whereQuery, function(error, results, field){
      if(error) throw error;
      // console.log(results)
      response['knownForTitle'] = [];
      for(let item in results){
        response['knownForTitle'].push(results[item]['primaryTitle'])
      }
      connection.query(sqlQueryProf + whereQuery, function(error, results, field){
        if(error) throw error;
        response['primaryProfession'] = [];
        for(let item in results){
          response['primaryProfession'].push(results[item]['profession'])
        }
        endTime = new Date().getTime();
        response['runningTime'] = endTime - startTime;
        res.send(response)
      })      
    })
  })
  app.get("/mysql/imdb/getGenres",(req, res)=>{
    connection.query("SELECT DISTINCT genres FROM `title_genres`", function(error, results, field){
      if(error) throw error;
      var response = {};
      response['result'] = results;
      res.send(response)
    })
  })
  app.post("/mysql/imdb/getMovie",(req, res)=>{
    startTime = new Date().getTime();
    sqlQuery = 'SELECT * from (select tid,primaryTitle,isAdult, startYear, runtimeMinutes, budget, `Gross US & Canada`, `Opening weekend US & Canada`, grossWorldwide, averageRating,numVotes, GROUP_CONCAT(genres) as genres from title_basics t1 LEFT JOIN title_profits t2 USING(tid) LEFT JOIN title_ratings t3 USING(tid) LEFT JOIN title_genres USING(tid) GROUP BY tid,primaryTitle,isAdult, startYear, runtimeMinutes, budget, `Gross US & Canada`, `Opening weekend US & Canada`, grossWorldwide, averageRating,numVotes ORDER BY tid)t'
    var condition = req.body.condition;
    console.log(condition)
    var whereQuery = '';
    if(Object.keys(condition).length > 0){
      whereQuery = ' WHERE '
      for(let i = 0; i < Object.keys(condition).length; i++){
        let item = Object.keys(condition)[i]
        if(item == 'genres'){
          for(let j = 0; j < condition[item].length; j++){
            if(j != condition[item].length - 1){
              whereQuery += item + " LIKE '%"+ condition[item][j] + "%' OR "
            }
            else if(i == Object.keys(condition).length - 1){
              whereQuery += item + " LIKE '%"+ condition[item][j] + "%'"
            }
            else{
              whereQuery += item + " LIKE '%"+ condition[item][j] + "%' AND "
            }
          }
        }
        else if(i != Object.keys(condition).length - 1)
          whereQuery += item + " LIKE '%"+ condition[item] + "%' AND "
        else
        whereQuery += item + " LIKE '%"+ condition[item] + "%'"
      }
    }
    sql = sqlQuery + whereQuery + ' LIMIT ' + req.body.pageFrom + ',' + req.body.pageSize
    console.log(sql)
    connection.query(sql, function(error, results, field){
      if(error) throw error;
      endTime = new Date().getTime();
      var response = {};
      response['runningTime'] = endTime - startTime;
      response['result'] = results;
      res.send(response)
    })
  })

  app.post("/mysql/imdb/getMovieDetail",(req, res)=>{
    startTime = new Date().getTime();
    sqlQueryPrincipal = 'SELECT title_principals.*, name_basics.primaryName FROM title_principals LEFT JOIN name_basics USING(nid)';
    sqlQueryAka = "select * from (SELECT tid, title, region, language, GROUP_CONCAT(types) AS types from title_akas JOIN title_akas_types USING(tid, ordering) GROUP BY tid, title, region, language)a"
    sqlQueryReview = 'SELECT * FROM (SELECT * from title_reviews order by reviewDate DESC)a'
    var tid = req.body.tid;
    var whereQuery = " WHERE tid = '" +tid+ "'";
    var response = {};
    connection.query(sqlQueryPrincipal + whereQuery, function(error, results, field){
      if(error) throw error;
      // console.log(results)
      response['Casts'] = results;
      connection.query(sqlQueryAka + whereQuery, function(error, results, field){
        if(error) throw error;
        response['AKAs'] = results;
          connection.query(sqlQueryReview + whereQuery, function(error, results, field){
            if(error) throw error;
            response['Reviews'] = results;
            endTime = new Date().getTime();
            response['runningTime'] = endTime - startTime;
            res.send(response)
          })       
      })      
    })
  })

  app.post("/mysql/imdb/recommendMovies",(req, res)=>{
    startTime = new Date().getTime();
    var tids = req.body.tids;
    var tidsql = " ("
    for(let i = 0; i < tids.length; i++){
      if(i != tids.length - 1){
        tidsql += "'" +tids[i] + "',"
      }
      else{
        tidsql += "'" +tids[i] + "')"
      }
    }
    sqlQueryPart1 = 'WITH counts_rating_title AS(SELECT match_tid, COUNT( match_tid) AS counts, title_ratings.averageRating, title_ratings.numVotes, CASE WHEN numVotes < 100 OR ISNULL(averageRating) THEN 0 ELSE averageRating END AS ratingRevalued FROM (SELECT pg.*, title_genres.tid AS match_tid FROM title_genres JOIN (SELECT * FROM title_genres WHERE tid IN ';
    sqlQueryPart2 = ") pg USING( genres) ORDER BY genres ) t LEFT JOIN title_ratings ON match_tid = title_ratings.tid WHERE match_tid NOT IN "
    sqlQueryPart3 = 'GROUP BY match_tid, title_ratings.averageRating, title_ratings.numVotes ORDER BY counts DESC) SELECT match_tid, t.primaryTitle, counts AS match_counts, ratingRevalued,(counts - min_counts)/(max_counts - min_counts) AS normalized_counts, (ratingRevalued - min_rating)/(max_rating - min_rating) AS normalized_rating, (counts - min_counts)/(max_counts - min_counts) * 0.8 + (ratingRevalued - min_rating)/(max_rating - min_rating) * 0.2 AS final_score, numVotes FROM (SELECT MIN(counts)as min_counts, MAX(counts) AS max_counts, MIN(ratingRevalued) AS min_rating, MAX(ratingRevalued) AS max_rating FROM counts_rating_title)min_max, counts_rating_title JOIN title_basics t ON match_tid = t.tid ORDER BY final_score DESC LIMIT 20'
    var response = {};
    connection.query(sqlQueryPart1 + tidsql + sqlQueryPart2 + tidsql + sqlQueryPart3, function(error, results, field){
      if(error) throw error;
      console.log(results)
      response['result'] = results;
      endTime = new Date().getTime();
      response['runningTime'] = endTime - startTime;
      res.send(response)    
    })
  })


  //MongoDB Part
  const MongoClient = require('mongodb').MongoClient
  const mongourl = 'mongodb://localhost:27017/imdb_mongo'

  app.post("/mongo/imdb/getName",(req, res)=>{
    startTime = new Date().getTime();
    var condition = req.body.condition;
    let findParam = {}
    if(Object.keys(condition).length > 0){
      for(let i = 0; i < Object.keys(condition).length; i++){
        let item = Object.keys(condition)[i]
        value = new RegExp(condition[item],'i')
        findParam[item] = {$regex : value}
      }
    }
    MongoClient.connect(mongourl, (err, db) => {
      if (err) throw err
      var dbbase = db.db('imdb_mongo')
      console.log(findParam)
      dbbase.collection('name_basics').find(findParam,{_id:0}).skip(req.body.pageFrom).limit(req.body.pageSize).toArray((err,results)=>{
        if(err)throw err;
        var response = {};
        response['result'] = results;
        endTime = new Date().getTime();
        response['runningTime'] = endTime - startTime;
        res.send(response)
        db.close();
      })
    })
  })

  app.get("/mongo/imdb/getGenres",(req, res)=>{
    MongoClient.connect(mongourl, (err, db) => {
      if (err) throw err
      var dbbase = db.db('imdb_mongo')
      dbbase.collection('title_basics').aggregate([
        {
            $unwind: '$genres'
        },
        {
          $group:{
            _id:'$genres'
          }
        }

    ]).toArray((err,results)=>{
        if(err)throw err;
        var response = {};
        let array = []
        results.forEach(row=>{
          row["genres"] = row["_id"]
        })
        response['result'] = results;
        res.send(response)
        db.close();
      })
    })
  })

  app.post("/mongo/imdb/getMovie",(req, res)=>{
    startTime = new Date().getTime();
    var condition = req.body.condition;
    let findParam = {}
    if(Object.keys(condition).length > 0){
      for(let i = 0; i < Object.keys(condition).length; i++){
        let item = Object.keys(condition)[i]
        value = new RegExp(condition[item],'i')
        findParam[item] = {$regex : value}
      }
    }
    MongoClient.connect(mongourl, (err, db) => {
      if (err) throw err
      var dbbase = db.db('imdb_mongo')
      console.log(findParam)
      dbbase.collection('title_basics').find(findParam,{_id:0}).skip(req.body.pageFrom).limit(req.body.pageSize).toArray((err,results)=>{
        if(err)throw err;
        var response = {};
        response['result'] = results;
        endTime = new Date().getTime();
        response['runningTime'] = endTime - startTime;
        res.send(response)
        db.close();
      })
    })
  })

  app.post("/mongo/imdb/getReviewsCasts",(req, res)=>{
    startTime = new Date().getTime();
    var rids = req.body.reviewIDs;
    var nids = req.body.nids;
    MongoClient.connect(mongourl, (err, db) => {
      if (err) throw err
      var dbbase = db.db('imdb_mongo')
      dbbase.collection('title_review').find({'data-review-id':{$in:rids}}).toArray((err,results)=>{
        if(err)throw err;
        var response = {};
        response['reviewResult'] = results;
        dbbase.collection('name_basics').find({'nid':{$in:nids}}).toArray((err,results)=>{
          response['castResult'] = results;
          endTime = new Date().getTime();
          response['runningTime'] = endTime - startTime;
          res.send(response)
          db.close();          
        })

      })
    })
  })

  app.post("/mongo/imdb/commonSQL",(req, res)=>{
    startTime = new Date().getTime();
    var query = req.body.mongoQuery;
    // console.log("query", query)
    MongoClient.connect(mongourl, (err, dbb) => {
      if (err) throw err
      var db = dbb.db('imdb_mongo')
      eval(query).skip(req.body.pageFrom).limit(req.body.pageSize).toArray((err,results)=>{
        if(err)throw err;
        // console.log(results)
        var response = {};
        response['result'] = results;
        endTime = new Date().getTime();
        response['runningTime'] = endTime - startTime;
        res.send(response)
        dbb.close();
      })
    })
  })

  app.post("/mongo/imdb/recommendMovies",(req, res)=>{
    startTime = new Date().getTime();
    var tids = req.body.tids;
    var selectedMovies = tids;
    MongoClient.connect(mongourl, (err, db) => {
      if (err) throw err
      var dbbase = db.db('imdb_mongo')
      dbbase.collection('title_basics').aggregate([
        {
            $match: {
                tid: { $in: selectedMovies }
            }
        },
        {
            $unwind: '$genres'
        },
        {
            $group: {
                _id: null,
                genres: { $addToSet: '$genres' }
            }
        }
    ]).toArray((err,results)=>{
        selectedGenres = results[0]["genres"];
        dbbase.collection('title_basics').aggregate([
          {
              $match: {
                  genres: { $in: selectedGenres },
                  tid: { $nin: selectedMovies },
                  averageRating: { $ne: null },
                  numVotes: { $gte: 100 }
              }
          },
          {
              $group: {
                  _id: '$tid',
                  genres: { $first: '$genres' },
                  averagerating: { $first: '$averageRating' },
                  intersection: {
                      $addToSet: {
                          $size: {
                              $setIntersection: ['$genres', selectedGenres]
                          }
                      }
                  }
              }
          },
          {
              $unwind: '$intersection'
          },
          {
              $group: {
                  _id: null,
                  minrating: { $min: '$averagerating' },
                  maxrating: { $max: '$averagerating' },
                  minc: { $min: '$intersection' },
                  maxc: { $max: '$intersection' }
              }
          }
      ]).toArray((err,results)=>{
        var minRating = results[0].minrating;
        var maxRating = results[0].maxrating;
        var minCount = results[0].minc;
        var maxCount = results[0].maxc;
        dbbase.collection('title_basics').aggregate([
          {
              $match: {
                  genres: { $in: selectedGenres },
                  tid: { $nin: selectedMovies },
                  averageRating: { $ne: null },
                  numVotes: { $gte: 100 }
              }
          },
           {
              $group: {
                  _id: '$tid',
                  primaryTitle:{$first:'$primaryTitle'},
                  genres: { $first: '$genres' },
                  averagerating: { $first: '$averageRating' },
                  intersection: {
                      $addToSet: {
                          $size: {
                              $setIntersection: ['$genres', selectedGenres]
                          }
                      }
                  }
              }
          },
          {
              $unwind: '$intersection'
          },
          {
              $project: {
                  _id: 0,
                  match_tid: '$_id',
                  primaryTitle:1,
                  genres: 1 ,
                  counts: '$intersection',
                  averagerating: 1
              }
          },
          {
              $addFields: {
                  normalized_rating: {
                      $divide: [
                          { $subtract: ['$averagerating', minRating] },
                          { $subtract: [maxRating, minRating] }
                      ]
                  },
                  normalized_counts: {
                      $divide: [
                          { $subtract: ['$counts', minCount] },
                          { $subtract: [maxCount, minCount] }
                      ]
                  }
              }
          },
          {
              $addFields: {
                  final_score: {
                      $add: [
                          { $multiply: ['$normalized_rating', 0.2] },
                          { $multiply: ['$normalized_counts', 0.8] }
                      ]
                  }
              }
          },
          {
              $sort: { final_score: -1 }
          },
          {
              $limit: 20
          }
      ]).toArray((err,results)=>{
        // console.log(results)
        var response = {};
        response['result'] = results;
        endTime = new Date().getTime();
        response['runningTime'] = endTime - startTime;
        res.send(response)
        db.close();
      })

      })
    })
    })
  })
